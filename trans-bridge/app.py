# audio-chunk-bridge/app.py
import os
import json
from pathlib import Path
import shutil
import time
from fastapi import FastAPI, Response, HTTPException
import uvicorn
import asyncio
import uuid # For generating a unique worker ID for claims

# External client imports
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from minio import Minio
from minio.error import S3Error

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092')
KAFKA_TOPIC_AUDIO_CHUNKS = os.environ.get('KAFKA_TOPIC_AUDIO_CHUNKS', 'audio-chunks')
KAFKA_TOPIC_TRANSCRIPTION_RESULTS = os.environ.get('KAFKA_TOPIC_TRANSCRIPTION_RESULTS', 'transcription-results')

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
MINIO_BUCKET_NAME = os.environ.get('MINIO_BUCKET_NAME', 'audio-chunks')


# --- MinIO Client Setup ---
minio_client = None
def get_minio_client():
    global minio_client
    if minio_client is None:
        if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
            raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set as environment variables for MinIO connection.")
        print(f"Initializing MinIO Client for: {MINIO_ENDPOINT}")
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
    return minio_client

# --- Kafka Consumer Setup ---
consumer = None
def get_kafka_consumer():
    global consumer
    if consumer is None:
        print(f"Initializing Kafka Consumer for topic '{KAFKA_TOPIC_AUDIO_CHUNKS}', group 'audio-chunk-bridge-group'")
        consumer = KafkaConsumer(
            KAFKA_TOPIC_AUDIO_CHUNKS,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            group_id='audio-chunk-bridge-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    return consumer

# --- Kafka Producer Setup (for results) ---
producer = None
def get_kafka_producer():
    global producer
    if producer is None:
        print(f"Initializing Kafka Producer for results topic: {KAFKA_TOPIC_TRANSCRIPTION_RESULTS}")
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            linger_ms=100
        )
    return producer

# In-memory storage for pending chunks.
# Maps chunk_id to a dictionary of metadata, local file path, status, and claim info.
# Status: "pending" | "processing" | "completed" | "failed"
# last_claimed_at: Timestamp when it was claimed (for potential timeout/re-claim)
# claimed_by: ID of the transcriber that claimed it
pending_chunks = {}

# Lock to prevent race conditions when multiple transcribers claim a chunk simultaneously.
claim_lock = asyncio.Lock()

# --- FastAPI App Definition ---
app = FastAPI()

@app.get("/audio-chunks/{chunk_id}")
async def get_audio_chunk(chunk_id: str):
    """
    Exposes a specific audio chunk by its ID for download.
    Does NOT modify status here. Status is managed by /pending-chunks/claim.
    """
    chunk_info = pending_chunks.get(chunk_id)
    if not chunk_info:
        print(f"Chunk {chunk_id} not found in pending_chunks (might be completed or invalid request).")
        raise HTTPException(status_code=404, detail="Chunk not found or invalid request.")

    # Only serve if it's currently being processed or was just claimed
    if chunk_info.get("status") not in ["processing", "pending"]:
        print(f"Chunk {chunk_id} status is {chunk_info.get('status')}. Not available for download.")
        raise HTTPException(status_code=403, detail="Chunk not in a downloadable state.")


    file_path = Path(chunk_info['local_chunk_path'])
    if not file_path.exists():
        print(f"Chunk file {file_path} not found on disk for chunk {chunk_id}.")
        raise HTTPException(status_code=404, detail="Chunk file not found on disk.")

    # Stream the file content
    try:
        with open(file_path, "rb") as f:
            content = f.read()
        print(f"Served chunk {chunk_id} from {file_path}")
        return Response(content=content, media_type="audio/mpeg", headers={"Content-Disposition": f"attachment; filename={chunk_info['chunk_filename']}"})
    except Exception as e:
        print(f"Error serving chunk {chunk_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error while serving chunk: {e}")

@app.post("/pending-chunks/claim")
async def claim_next_chunk():
    """
    Allows a transcriber to claim the next available 'pending' chunk.
    This operation is protected by a lock to ensure atomicity.
    Returns the chunk's info or 404 if no pending chunks.
    """
    async with claim_lock:
        # This block is now thread-safe. Only one request can execute this code at a time.
        for chunk_id, info in pending_chunks.items():
            if info.get("status", "pending") == "pending":
                info["status"] = "processing"
                info["last_claimed_at"] = time.time()
                info["claimed_by"] = str(uuid.uuid4()) # Assign a unique claim ID for this request
                print(f"Claimed chunk {chunk_id} for processing by {info['claimed_by']}. Status: processing.")
                return {
                    "chunk_id": chunk_id,
                    "metadata": info["metadata"],
                    "claimed_by": info["claimed_by"],
                    "generation_config": info.get("generation_config", {}), # Return config to transcriber
                    "user_id": info.get("user_id"),
                }
    
    # If the lock is released and we haven't returned, it means no chunks were found.
    raise HTTPException(status_code=404, detail="No pending chunks available to claim.")


@app.post("/chunks/{chunk_id}/processed")
async def mark_chunk_processed(chunk_id: str, claimed_by: str = None): # Add claimed_by to ensure correct claim
    """
    Endpoint for a local transcriber to signal that a chunk has been processed.
    This will trigger cleanup.
    """
    chunk_info = pending_chunks.get(chunk_id)
    if not chunk_info:
        raise HTTPException(status_code=404, detail="Chunk not found or already processed.")
    
    # Optional: Verify it was claimed by the correct worker instance
    if claimed_by and chunk_info.get("claimed_by") != claimed_by:
        print(f"Warning: Chunk {chunk_id} processed signal received from unexpected claimant. Expected {chunk_info.get('claimed_by')}, got {claimed_by}.")
        # Decide if you want to allow it or deny. For now, allow but warn.
        # raise HTTPException(status_code=403, detail="Claim mismatch.")
    
    # Mark the chunk as 'completed'
    chunk_info["status"] = "completed"
    print(f"Chunk {chunk_id} marked as 'completed' by external service (claimant: {claimed_by}).")

    # Clean up the file after it's confirmed processed
    file_path = Path(chunk_info['local_chunk_path'])
    if file_path.exists():
        shutil.rmtree(file_path.parent, ignore_errors=True)
        print(f"Cleaned up temporary chunk directory for processed chunk: {file_path.parent}")
    
    # Remove from pending_chunks
    del pending_chunks[chunk_id]
    print(f"Removed chunk {chunk_id} from pending_chunks.")
    
    return {"status": "success", "message": f"Chunk {chunk_id} marked as processed and cleaned up."}


# Background task to consume Kafka messages
async def consume_kafka_chunks():
    consumer = get_kafka_consumer()
    loop = asyncio.get_event_loop()
    print("Starting Kafka consumer background task for bridge service...")
    minio_client_instance = get_minio_client()

    while True:
        try:
            # Run the blocking poll operation in a separate thread to avoid freezing the event loop
            messages = await loop.run_in_executor(
                None, consumer.poll, 1000, 1  # Corresponds to timeout_ms=1000, max_records=1
            )

            if not messages:
                await asyncio.sleep(1) # Wait a bit if no messages are available
                continue

            for tp, consumer_records in messages.items():
                for message in consumer_records:
                    chunk_metadata = message.value
                    chunk_id = chunk_metadata.get('chunk_id')
                    
                    if chunk_id in pending_chunks:
                        print(f"Bridge Service: Chunk {chunk_id} already in pending_chunks. Skipping re-processing.")
                        continue

                    print(f"\nBridge Service: Received Kafka message for chunk: {chunk_id}")

                    temp_chunk_dir = Path(f"/tmp/bridge_chunks/{chunk_metadata['video_id']}/{chunk_id}")
                    temp_chunk_dir.mkdir(parents=True, exist_ok=True)
                    local_chunk_path = temp_chunk_dir / chunk_metadata['chunk_filename']

                    try:
                        minio_object_name = "/".join(chunk_metadata['chunk_url'].split("//")[1].split("/")[1:])
                        minio_client_instance.fget_object(MINIO_BUCKET_NAME, minio_object_name, str(local_chunk_path))
                        print(f"Bridge Service: Downloaded '{minio_object_name}' to '{local_chunk_path}'")
                        
                        pending_chunks[chunk_id] = {
                            "local_chunk_path": str(local_chunk_path),
                            "chunk_filename": chunk_metadata['chunk_filename'],
                            "metadata": chunk_metadata,
                            "status": "pending",
                            "generation_config": chunk_metadata.get("generation_config", {}),
                            "user_id": chunk_metadata.get("user_id"),
                        }
                        print(f"Bridge Service: Chunk {chunk_id} ready for external consumption via API. Status: pending.")

                    except S3Error as e:
                        print(f"Bridge Service: Error downloading '{minio_object_name}' from MinIO: {e}")
                        shutil.rmtree(temp_chunk_dir, ignore_errors=True)
                    except Exception as e:
                        print(f"Bridge Service: Unexpected error during processing chunk {chunk_id}: {type(e).__name__}: {e}")
                        shutil.rmtree(temp_chunk_dir, ignore_errors=True)

        except KafkaError as outer_e:
            print(f"CRITICAL KAFKA ERROR in bridge service consumer loop: {type(outer_e).__name__}: {outer_e}")
            await asyncio.sleep(5)
        except Exception as outer_e:
            print(f"CRITICAL GENERAL ERROR in bridge service consumer loop: {type(outer_e).__name__}: {outer_e}")
            await asyncio.sleep(5)

# Startup event to run the Kafka consumer in the background
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume_kafka_chunks())
    print("Bridge Service: Startup tasks initiated.")

# Shutdown event to close Kafka consumer/producer gracefully
@app.on_event("shutdown")
async def shutdown_event():
    if consumer:
        print("Bridge Service: Closing Kafka consumer.")
        consumer.close()
    if producer:
        print("Bridge Service: Closing Kafka producer.")
        producer.close()


if __name__ == "__main__":
    print("Starting Audio Chunk Bridge Service...")
    get_minio_client()
    get_kafka_producer()
    uvicorn.run(app, host="0.0.0.0", port=8001)