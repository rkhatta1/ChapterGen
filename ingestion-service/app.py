# ingestion-service/app.py
import os
import subprocess
import time
from pathlib import Path
import json
import uuid
from uuid import uuid4
import shutil
import asyncio
from fastapi.responses import JSONResponse
from fastapi import FastAPI, Body, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Dict
import uvicorn
import httpx

# External client imports
from kafka import KafkaProducer
from minio import Minio
from minio.error import S3Error

# --- Minikube Deployment Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092')
KAFKA_TOPIC_AUDIO_CHUNKS = os.environ.get('KAFKA_TOPIC_AUDIO_CHUNKS', 'audio-chunks')

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'minio:9000') # MinIO is in default namespace
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
MINIO_BUCKET_NAME = os.environ.get('MINIO_BUCKET_NAME', 'audio-chunks')
DATABASE_SERVICE_URL = "http://database-service-service:8008"


CHUNK_DURATION = int(os.environ.get('CHUNK_DURATION_SECONDS', '60'))  # seconds
CHUNK_OVERLAP = int(os.environ.get('OVERLAP_SECONDS', '10'))  # seconds

# --- Kafka Producer Setup ---
producer = None
def get_kafka_producer():
    global producer
    if producer is None:
        print(f"Initializing Kafka Producer for: {KAFKA_BOOTSTRAP_SERVERS}")
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            linger_ms=100
        )
    return producer

# --- MinIO Client Setup ---
minio_client = None
def get_minio_client():
    global minio_client
    if minio_client is None:
        if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
            raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set as environment variables.")
        print(f"Initializing MinIO Client for: {MINIO_ENDPOINT}")
        from minio import Minio
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
    return minio_client

def ensure_minio_bucket_exists():
    client = get_minio_client()
    try:
        from minio.error import S3Error
        found = client.bucket_exists(MINIO_BUCKET_NAME)
        if not found:
            client.make_bucket(MINIO_BUCKET_NAME)
            print(f"Created MinIO bucket '{MINIO_BUCKET_NAME}'")
        else:
            print(f"MinIO bucket '{MINIO_BUCKET_NAME}' already exists")
    except S3Error as e:
        print(f"Error checking/creating MinIO bucket: {e}")
        raise

def upload_chunk_to_minio(bucket_name: str, file_path: Path, object_name: str):
    client = get_minio_client()
    try:
        client.fput_object(
            bucket_name,
            object_name,
            str(file_path),
            content_type="audio/mpeg"
        )
        print(f"Uploaded '{file_path.name}' to MinIO as '{bucket_name}/{object_name}'")
        return f"s3://{bucket_name}/{object_name}"
    except S3Error as e:
        print(f"Error uploading '{file_path.name}' to MinIO: {e}")
        raise

def publish_kafka_message(topic: str, message: dict):
    prod = get_kafka_producer()
    try:
        prod.send(topic, message)
        prod.flush()
        print(f"Published Kafka message to topic '{topic}': {json.dumps(message, indent=2)}")
    except Exception as e:
        print(f"Error publishing message to Kafka topic '{topic}': {e}")
        raise

def download_and_chunk_audio(youtube_url: str, user_id: int, generation_config: Optional[Dict] = None, job_id: Optional[str] = None):
    video_id = youtube_url.split("v=")[-1].split("&")[0]

    print(f"\n ---- Processing YT URL: {youtube_url} (ID: {video_id}) ---- \n")

    temp_dir = Path(f"/tmp/temp_audio/{video_id}")
    temp_dir.mkdir(parents=True, exist_ok=True)

    audio_path = temp_dir / f"{video_id}_full.mp3"

    download_cmd= [
        "yt-dlp", "-x", "--audio-format", "mp3", "--embed-metadata", "-o", str(audio_path), youtube_url
    ]
    print(f"Running download command: {' '.join(download_cmd)}")

    try:
        subprocess.run(download_cmd, check=True, capture_output=True, text=True)
        print(f"Downloaded audio to {audio_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error downloading audio: {e.stderr}")
        return False
    except FileNotFoundError:
        print("yt-dlp not found. Please ensure yt-dlp is installed in the container and in PATH.")
        return False
    except Exception as e:
        print(f"Unexpected error during yt-dlp download: {e}")
        return False

    ffprobe_cmd = [
        "ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", str(audio_path)
    ]
    try:
        duration_output = subprocess.check_output(ffprobe_cmd, text=True).strip()
        total_duration = float(duration_output)
        print(f"Audio duration: {total_duration:.2f} seconds")
    except (subprocess.CalledProcessError, ValueError) as e:
        print(f"Error getting audio duration with ffprobe: {e}")
        return False
    except FileNotFoundError:
        print("ffprobe not found. Please ensure ffmpeg is installed in the container and in PATH.")
        return False
    except Exception as e:
        print(f"Unexpected error during ffprobe duration check: {e}")
        return False

    # Calculate step size and total_chunks ahead of the chunking loop
    step = CHUNK_DURATION - CHUNK_OVERLAP
    if step <= 0:
        print("Invalid chunking configuration: CHUNK_DURATION must be > CHUNK_OVERLAP")
        return False

    # compute total number of chunks by simulating the loop
    _total = 0
    _temp_start = 0.0
    while _temp_start < total_duration:
        _total += 1
        _temp_start += step
    total_chunks = _total
    print(f"Computed total_chunks = {total_chunks} for duration {total_duration:.2f}s, "
          f"chunk_duration={CHUNK_DURATION}, overlap={CHUNK_OVERLAP}")

    start_time = 0.0
    chunk_index = 0

    while start_time < total_duration:
        chunk_id = str(uuid.uuid4())
        chunk_filename = f"{video_id}_chunk_{chunk_index}.mp3"
        chunk_path = temp_dir / chunk_filename

        current_chunk_end_time_sec = min(start_time + CHUNK_DURATION, total_duration)
        actual_chunk_duration = current_chunk_end_time_sec - start_time

        ffmpeg_cmd = [
            "ffmpeg", "-i", str(audio_path), "-ss", str(start_time), "-t", str(actual_chunk_duration),
            "-c:a", "libmp3lame", "-q:a", "2", "-map_metadata", "-1", str(chunk_path)
        ]

        print(f"Running ffmpeg command for chunk {chunk_index} (start: {start_time:.2f}, duration: {actual_chunk_duration:.2f}, end: {current_chunk_end_time_sec:.2f}): {' '.join(ffmpeg_cmd)}")

        try:
            subprocess.run(ffmpeg_cmd, check=True, capture_output=True, text=True)
            print(f"Created chunk: {chunk_path}")
        except subprocess.CalledProcessError as e:
            print(f"Error creating chunk {chunk_index}: {e.stderr}")
            start_time += CHUNK_DURATION - CHUNK_OVERLAP
            chunk_index += 1
            continue
        except FileNotFoundError:
            print("ffmpeg not found. Please ensure ffmpeg is installed in the container and in PATH.")
            return False
        except Exception as e:
            print(f"Unexpected error creating chunk {chunk_index}: {e}")
            return False

        object_name = f"{video_id}/chunks/{chunk_filename}"
        try:
            minio_url = upload_chunk_to_minio(MINIO_BUCKET_NAME, chunk_path, object_name)
            print(f"MinIO upload complete: {minio_url}")
        except Exception as e:
            print(f"Failed to upload chunk {chunk_index} to MinIO: {e}")
            start_time += CHUNK_DURATION - CHUNK_OVERLAP
            chunk_index += 1
            continue

        message = {
            "video_id": video_id,
            "job_id": job_id,
            "chunk_id": chunk_id,
            "chunk_index": chunk_index,
            "total_chunks": total_chunks,
            "chunk_filename": chunk_filename,
            "chunk_url": minio_url,
            "start_time_sec": start_time,
            "end_time_sec": current_chunk_end_time_sec,
            "generation_config": generation_config or {}, # Pass config along
            "user_id": user_id
        }
        try:
            publish_kafka_message(KAFKA_TOPIC_AUDIO_CHUNKS, message)
        except Exception as e:
            print(f"Failed to publish Kafka message for chunk {chunk_id}: {e}")
            pass

        start_time += step
        chunk_index += 1

    print(f"\n ---- Cleaning up temp directory: {temp_dir} ---- \n")
    try:
        shutil.rmtree(temp_dir)
        print(f"Removed temporary directory: {temp_dir}")
    except OSError as e:
        print(f"Error removing temp directory {temp_dir}: {e}")
    return True

# new helper: background runner for the blocking ingestion work
async def _run_ingestion_job(youtube_url: str, user_id: int,
                             generation_config: dict, job_id: str):
    """
    Background runner that:
      - sets job status -> processing
      - runs blocking download_and_chunk_audio in a thread
      - updates job status -> completed/failed
    """
    # mark job as processing in DB (best-effort)
    try:
        async with httpx.AsyncClient() as client:
            await client.patch(
                f"{DATABASE_SERVICE_URL}/jobs/{job_id}",
                json={"status": "processing"},
                timeout=30.0
            )
    except Exception as e:
        print(f"Warning: failed to set job {job_id} to processing: {e}")

    try:
        # Run the blocking function in a thread so we don't block the event loop
        success = await asyncio.to_thread(
            download_and_chunk_audio,
            youtube_url,
            user_id,
            generation_config,
            job_id,
        )
        final_status = "completed" if success else "failed"
    except Exception as e:
        print(f"Background ingestion job {job_id} raised exception: {type(e).__name__}: {e}")
        final_status = "failed"

    # update final status in DB (best-effort)
    try:
        async with httpx.AsyncClient() as client:
            await client.patch(
                f"{DATABASE_SERVICE_URL}/jobs/{job_id}",
                json={"status": final_status},
                timeout=30.0
            )
    except Exception as e:
        print(f"Warning: failed to update final status for job {job_id}: {e}")

# --- Pydantic Models for Request Body ---
class GenerationConfig(BaseModel):
    creativity: Optional[str] = 'Neutral'
    segmentation_threshold: Optional[str] = 'Default'

class ProcessRequest(BaseModel):
    youtube_url: str
    generation_config: Optional[GenerationConfig] = Field(default_factory=dict)
    access_token: str
    video_details: dict

# --- FastAPI App Definition ---
app = FastAPI()

# --- CORS Middleware Setup ---
# This allows the frontend (running on a different origin) to communicate with this API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"]  # Allows all methods
)

async def get_user_id_from_token(access_token: str):
    """Get user ID from the database service using the access token."""
    try:
        async with httpx.AsyncClient() as client:
            # Get profile from Google
            headers = {"Authorization": f"Bearer {access_token}"}
            response = await client.get("https://www.googleapis.com/oauth2/v1/userinfo", headers=headers)
            response.raise_for_status()
            profile = response.json()
            email = profile.get("email")

            if not email:
                return None, None

            # Get user from DB by email
            response = await client.get(f"{DATABASE_SERVICE_URL}/users/by-email/{email}")
            response.raise_for_status()
            user_data = response.json()
            return user_data.get("id"), user_data.get("email")
    except httpx.RequestError as e:
        print(f"Error getting user ID: {e}")
        return None, None

async def check_job_exists(video_id: str):
    """Checks if a job already exists for a given video_id."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{DATABASE_SERVICE_URL}/jobs/by-video-id/{video_id}")
            if response.status_code == 200:
                job_data = response.json()
                # Check if the job has already been completed
                if job_data.get("status") == "completed":
                    return True, "This video has already been processed."
                return True, "This video is currently being processed."
            return False, ""
    except httpx.RequestError:
        # If the database service is down or there's a network error, allow the job to proceed
        # This is a fallback to ensure the service remains available
        return False, ""

# replace your existing endpoint with this updated async endpoint
@app.post("/process-youtube-url/")
async def process_youtube_url_endpoint(request: ProcessRequest):
    """
    Accepts a YouTube URL and generation config, creates a queued job,
    schedules ingestion in the background, and returns HTTP 202 with job_id.
    """
    print(f"Received request to process URL: {request.youtube_url}")
    print(f"Generation config: {request.generation_config}")

    access_token = request.access_token
    if not access_token:
        return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST,
                            content={"status": "failure", "message": "Access token is required."})

    # authenticate user (via Google profile -> DB lookup)
    user_id, user_email = await get_user_id_from_token(access_token)
    if not user_id:
        return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED,
                            content={"status": "failure", "message": "Could not authenticate user."})

    video_id = request.video_details["id"]
    exists, message = await check_job_exists(video_id)
    if exists:
        return JSONResponse(status_code=status.HTTP_409_CONFLICT,
                            content={"status": "failure", "message": message})

    # Generate unique job_id (we return this immediately)
    job_id = str(uuid4())

    # Create a job record in DB with status 'queued'
    job_data = {
        "id": job_id,
        "video_id": video_id,
        "title": request.video_details["snippet"]["title"],
        "description": request.video_details["snippet"]["description"],
        "thumbnail_url": request.video_details["snippet"]["thumbnails"]["high"]["url"],
        "owner_email": user_email,
        "status": "queued",
    }
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{DATABASE_SERVICE_URL}/jobs/", json=job_data, timeout=30.0)
            response.raise_for_status()
    except httpx.RequestError as e:
        print(f"DB request error when creating job: {e}")
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            content={"status": "failure", "message": f"Could not create job in database: {e}"})
    except Exception as e:
        print(f"DB returned error when creating job: {e}")
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            content={"status": "failure", "message": f"Database returned error: {e}"})

    # Convert generation_config to dict for background processing
    config_dict = request.generation_config.dict() if request.generation_config else {}

    # Schedule background ingestion task (non-blocking)
    asyncio.create_task(_run_ingestion_job(request.youtube_url, user_id, config_dict, job_id))

    # Return early with 202 Accepted and job_id
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"status": "accepted", "job_id": job_id, "message": "Job queued."}
    )


if __name__ == "__main__":
    print("Starting Ingestion Service for Minikube Deployment...")

    try:
        ensure_minio_bucket_exists()
    except Exception as e:
        print(f"Critical error during MinIO bucket setup: {e}")
        print("Exiting. Check MinIO configuration and network connectivity.")
        exit(1)

    print("Ingestion Service ready. Starting FastAPI server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)