# trans-bridge/app.py (Cloud Run Trigger Version)
import os
import json
import asyncio
from aiokafka import AIOKafkaConsumer

# Import the Google Cloud Run client library
from google.cloud import run_v2

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092')
KAFKA_TOPIC_AUDIO_CHUNKS = os.environ.get('KAFKA_TOPIC_AUDIO_CHUNKS', 'audio-chunks')

# --- Google Cloud Configuration ---
# Your Google Cloud Project ID, e.g., "river-blade-467821-g4"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
# The region where your Cloud Run job is deployed, e.g., "us-central1"
GCP_REGION = os.environ.get("GCP_REGION", "us-central1")
# The name of the Cloud Run job you created
CLOUD_RUN_JOB_NAME = os.environ.get("CLOUD_RUN_JOB_NAME", "transcriber-service-job")
# The public URL of your main application server for the transcriber to call back to.
# This should be the external IP of your Google Cloud VM.
BRIDGE_PUBLIC_URL = os.environ.get("BRIDGE_PUBLIC_URL")

async def execute_cloud_run_job(chunk_data: dict):
    """Triggers the Cloud Run job to process a single chunk."""
    print(f"Triggering Cloud Run job for chunk: {chunk_data.get('chunk_id')}")
    client = run_v2.JobsAsyncClient()

    job_path = f"projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}/jobs/{CLOUD_RUN_JOB_NAME}"

    # Define environment variables to pass to the Cloud Run job
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

    request = run_v2.RunJobRequest(
        name=job_path,
        overrides=overrides
    )

    try:
        operation = await client.run_job(request=request)
        print(f"Successfully started Cloud Run job execution for chunk {chunk_data.get('chunk_id')}. Operation: {operation.operation.name}")
    except Exception as e:
        print(f"Error triggering Cloud Run job for chunk {chunk_data.get('chunk_id')}: {e}")


async def consume_kafka_chunks():
    """Consumes messages from Kafka and triggers a Cloud Run job for each one."""
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
            print(f"--- [KAFKA CONSUMER] Received Message: {message.value} ---")
            chunk_metadata = json.loads(message.value) # Removed .decode for python-kafka > 2.0
            await execute_cloud_run_job(chunk_metadata)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    if not all([GCP_PROJECT_ID, BRIDGE_PUBLIC_URL]):
        print("Error: GCP_PROJECT_ID and BRIDGE_PUBLIC_URL must be set as environment variables.")
    else:
        print("Starting transcription bridge worker...")
        asyncio.run(consume_kafka_chunks())