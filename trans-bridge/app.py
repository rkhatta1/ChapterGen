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
from aiokafka import AIOKafkaConsumer
from minio import Minio
from minio.error import S3Error

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092')
KAFKA_TOPIC_AUDIO_CHUNKS = os.environ.get('KAFKA_TOPIC_AUDIO_CHUNKS', 'audio-chunks')

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

# In-memory storage for pending chunks.
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
        raise HTTPException(status_code=404, detail="Chunk not found or invalid request.")

    if chunk_info.get("status") not in ["processing", "pending"]:
        raise HTTPException(status_code=403, detail="Chunk not in a downloadable state.")

    file_path = Path(chunk_info['local_chunk_path'])
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Chunk file not found on disk.")

    try:
        with open(file_path, "rb") as f:
            content = f.read()
        return Response(content=content, media_type="audio/mpeg", headers={"Content-Disposition": f"attachment; filename={chunk_info['chunk_filename']}"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error while serving chunk: {e}")

@app.post("/pending-chunks/claim")
async def claim_next_chunk():
    """
    Allows a transcriber to claim the next available 'pending' chunk.
    This operation is protected by a lock to ensure atomicity.
    """
    async with claim_lock:
        for chunk_id, info in pending_chunks.items():
            if info.get("status", "pending") == "pending":
                info["status"] = "processing"
                info["last_claimed_at"] = time.time()
                info["claimed_by"] = str(uuid.uuid4())
                print(f"Claimed chunk {chunk_id} for processing by {info['claimed_by']}.")
                return {
                    "chunk_id": chunk_id,
                    "metadata": info["metadata"],
                    "claimed_by": info["claimed_by"],
                    "generation_config": info.get("generation_config", {}),
                    "user_id": info.get("user_id"),
                }
    raise HTTPException(status_code=404, detail="No pending chunks available to claim.")


@app.post("/chunks/{chunk_id}/processed")
async def mark_chunk_processed(chunk_id: str, claimed_by: str = None):
    """
    Endpoint for a local transcriber to signal that a chunk has been processed.
    """
    chunk_info = pending_chunks.get(chunk_id)
    if not chunk_info:
        raise HTTPException(status_code=404, detail="Chunk not found or already processed.")
    
    if claimed_by and chunk_info.get("claimed_by") != claimed_by:
        print(f"Warning: Chunk {chunk_id} processed signal from unexpected claimant.")
    
    chunk_info["status"] = "completed"
    file_path = Path(chunk_info['local_chunk_path'])
    if file_path.exists():
        shutil.rmtree(file_path.parent, ignore_errors=True)
    
    del pending_chunks[chunk_id]
    print(f"Chunk {chunk_id} marked as processed and cleaned up.")
    return {"status": "success", "message": f"Chunk {chunk_id} marked as processed."}


# Background task to consume Kafka messages
async def consume_kafka_chunks():
    print("Starting Kafka consumer background task for bridge service...")
    minio_client_instance = get_minio_client()
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_AUDIO_CHUNKS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='audio-chunk-bridge-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    await consumer.start()

    try:
        async for message in consumer:
            chunk_metadata = message.value
            chunk_id = chunk_metadata.get('chunk_id')
            
            if chunk_id in pending_chunks:
                print(f"Bridge Service: Chunk {chunk_id} already pending. Skipping.")
                continue

            print(f"Bridge Service: Received Kafka message for chunk: {chunk_id}")

            temp_chunk_dir = Path(f"/tmp/bridge_chunks/{chunk_metadata['video_id']}/{chunk_id}")
            temp_chunk_dir.mkdir(parents=True, exist_ok=True)
            local_chunk_path = temp_chunk_dir / chunk_metadata['chunk_filename']

            try:
                minio_object_name = "/".join(chunk_metadata['chunk_url'].split("//")[1].split("/")[1:])
                minio_client_instance.fget_object(MINIO_BUCKET_NAME, minio_object_name, str(local_chunk_path))
                
                pending_chunks[chunk_id] = {
                    "local_chunk_path": str(local_chunk_path),
                    "chunk_filename": chunk_metadata['chunk_filename'],
                    "metadata": chunk_metadata,
                    "status": "pending",
                    "generation_config": chunk_metadata.get("generation_config", {}),
                    "user_id": chunk_metadata.get("user_id"),
                }
                print(f"Bridge Service: Chunk {chunk_id} ready for processing.")
            except S3Error as e:
                print(f"Bridge Service: MinIO download error for '{minio_object_name}': {e}")
                shutil.rmtree(temp_chunk_dir, ignore_errors=True)
            except Exception as e:
                print(f"Bridge Service: Unexpected error for chunk {chunk_id}: {e}")
                shutil.rmtree(temp_chunk_dir, ignore_errors=True)
    finally:
        await consumer.stop()

# Startup event to run the Kafka consumer in the background
@app.on_event("startup")
async def startup_event():
    get_minio_client() # Initialize MinIO client on startup
    asyncio.create_task(consume_kafka_chunks())
    print("Bridge Service: Startup tasks initiated.")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)