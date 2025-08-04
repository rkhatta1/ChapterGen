# trans-bridge/app.py (Final Hybrid Version)
import os
import json
import asyncio
from pathlib import Path
import shutil
import time
from fastapi import FastAPI, Response, HTTPException, Body
import uvicorn
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from minio import Minio
from minio.error import S3Error
from google.cloud import run_v2

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092')
KAFKA_TOPIC_AUDIO_CHUNKS = os.environ.get('KAFKA_TOPIC_AUDIO_CHUNKS', 'audio-chunks')
KAFKA_TOPIC_TRANSCRIPTION_RESULTS = os.environ.get('KAFKA_TOPIC_TRANSCRIPTION_RESULTS', 'transcription-results')

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
MINIO_BUCKET_NAME = os.environ.get('MINIO_BUCKET_NAME', 'audio-chunks')

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_REGION = os.environ.get("GCP_REGION", "us-central1")
CLOUD_RUN_JOB_NAME = os.environ.get("CLOUD_RUN_JOB_NAME", "transcriber-service-job")
BRIDGE_PUBLIC_URL = os.environ.get("BRIDGE_PUBLIC_URL")

# --- Clients ---
minio_client = None
kafka_producer = None
app = FastAPI()

# In-memory storage for chunk data passed to Cloud Run
# This is necessary so the bridge knows what file to serve from Minio
pending_chunks_info = {}

# --- Client Setup ---
def get_minio_client():
    global minio_client
    if minio_client is None:
        print(f"Initializing MinIO Client for: {MINIO_ENDPOINT}")
        minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    return minio_client

async def get_kafka_producer():
    global kafka_producer
    if kafka_producer is None:
        print("Initializing Kafka Producer...")
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await producer.start()
        kafka_producer = producer
    return kafka_producer

# --- API Endpoints ---
@app.get("/audio-chunks/{chunk_id}")
async def get_audio_chunk(chunk_id: str):
    """Serves the audio file directly from Minio."""
    chunk_info = pending_chunks_info.get(chunk_id)
    if not chunk_info:
        raise HTTPException(status_code=404, detail="Chunk info not found. It may have already been processed.")
    
    client = get_minio_client()
    try:
        minio_object_name = "/".join(chunk_info['chunk_url'].split("//")[1].split("/")[1:])
        print(f"Serving chunk {chunk_id} from Minio object: {minio_object_name}")
        response = client.get_object(MINIO_BUCKET_NAME, minio_object_name)
        return Response(content=response.read(), media_type="audio/mpeg")
    except S3Error as e:
        print(f"MinIO download error for '{minio_object_name}': {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve audio chunk from storage.")
    finally:
        if 'response' in locals() and response:
            response.close()
            response.release_conn()

@app.post("/chunks/{chunk_id}/transcribed")
async def receive_transcription_results(chunk_id: str, results: dict = Body(...)):
    """Receives transcription results and publishes them to Kafka."""
    producer = await get_kafka_producer()
    try:
        await producer.send_and_wait(KAFKA_TOPIC_TRANSCRIPTION_RESULTS, results)
        print(f"Successfully published transcription for chunk {chunk_id} to Kafka.")
        # Clean up the info for the processed chunk
        if chunk_id in pending_chunks_info:
            del pending_chunks_info[chunk_id]
        return {"status": "success"}
    except Exception as e:
        print(f"Error publishing transcription for chunk {chunk_id} to Kafka: {e}")
        raise HTTPException(status_code=500, detail="Failed to publish results to Kafka.")

async def execute_cloud_run_job(chunk_data: dict):
    """Triggers the Cloud Run job to process a single chunk."""
    chunk_id = chunk_data.get('chunk_id')
    print(f"--- [Cloud Run] Attempting to trigger job for chunk: {chunk_id} ---")

    try:
        print("--- [Cloud Run] Initializing JobsAsyncClient... ---")
        client = run_v2.JobsAsyncClient()
        print("--- [Cloud Run] Client initialized successfully. ---")

        job_path = f"projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}/jobs/{CLOUD_RUN_JOB_NAME}"

        overrides = run_v2.RunJobRequest.Overrides(
            container_overrides=[
                run_v2.RunJobRequest.Overrides.ContainerOverride(
                    env=[
                        {"name": "BRIDGE_SERVICE_URL", "value": BRIDGE_PUBLIC_URL},
                        {"name": "CHUNK_DATA_JSON", "value": json.dumps(chunk_data)}
                    ]
                )
            ]
        )

        request = run_v2.RunJobRequest(name=job_path, overrides=overrides)

        print(f"--- [Cloud Run] Sending run_job request for chunk {chunk_id}... ---")
        operation = await client.run_job(request=request)
        print(f"--- [Cloud Run] Successfully started job for chunk {chunk_id}. Operation: {operation.operation.name} ---")

    except Exception as e:
        print(f"--- [CRITICAL Cloud Run Error] Failed to execute job for chunk {chunk_id}. ---")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Details: {e}")
        traceback.print_exc()


async def consume_kafka_chunks():
    print("Starting Kafka consumer to trigger Cloud Run jobs...")
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_AUDIO_CHUNKS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='cloud-run-trigger-group',
        auto_offset_reset='earliest'
    )
    await consumer.start()
    try:
        async for message in consumer:
            print(f"--- [KAFKA CONSUMER] Received Message ---")
            chunk_metadata = json.loads(message.value)
            # Store chunk info in memory so it can be served via the API
            pending_chunks_info[chunk_metadata.get('chunk_id')] = chunk_metadata
            asyncio.create_task(execute_cloud_run_job(chunk_metadata))
    finally:
        await consumer.stop()

# --- FastAPI Lifespan Events ---
@app.on_event("startup")
async def startup_event():
    get_minio_client()
    await get_kafka_producer()
    asyncio.create_task(consume_kafka_chunks())
    print("Bridge Service: Startup tasks initiated.")

@app.on_event("shutdown")
async def shutdown_event():
    if kafka_producer:
        await kafka_producer.stop()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)