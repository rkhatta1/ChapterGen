# local_transcriber.py
import requests
import time
import os
import json
from pathlib import Path
import shutil
import uuid

# External client imports
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --- Whisper Local Model Import ---
import whisper

# Global model loading (done once when the app starts)
print("Loading Whisper model (this may take a moment)...")
try:
    # Adjust WHISPER_MODEL_SIZE as needed ("base", "small", etc.)
    # device="cpu" is good for most local machines if no powerful GPU
    whisper_model = whisper.load_model(os.environ.get("WHISPER_MODEL_SIZE", "small"), device="cuda")
    print(f"Whisper model '{os.environ.get('WHISPER_MODEL_SIZE', 'small')}' loaded successfully.")
except Exception as e:
    print(f"Failed to load Whisper model locally: {e}")
    exit(1)

# --- Configuration (Local Machine) ---
# Each local transcriber instance should ideally have a unique ID for logging/tracking
LOCAL_TRANSCRIBER_ID = str(uuid.uuid4())[:8]
print(f"Local Transcriber Instance ID: {LOCAL_TRANSCRIBER_ID}")
# This is where your local listener will receive chunk IDs from the bridge service
BRIDGE_SERVICE_URL = "http://127.0.0.1:8001" # After port-forwarding the bridge service
# Or if you use minikube tunnel or host network: "http://<minikube-ip>:8000"

# Kafka config for sending results back to the cluster
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '192.168.39.8:30907') # Will need port-forward or minikube tunnel for Kafka
KAFKA_TOPIC_TRANSCRIPTION_RESULTS = os.environ.get('KAFKA_TOPIC_TRANSCRIPTION_RESULTS', 'transcription-results')

# --- Kafka Producer Setup (for results) ---
producer = None
def get_kafka_producer():
    global producer
    if producer is None:
        print(f"Initializing Local Kafka Producer for results topic: {KAFKA_TOPIC_TRANSCRIPTION_RESULTS}")
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            linger_ms=100
        )
    return producer

def publish_kafka_message(topic: str, message: dict):
    prod = get_kafka_producer()
    try:
        prod.send(topic, message)
        prod.flush()
        print(f"Published Kafka message from local transcriber to topic '{topic}': {json.dumps(message, indent=2)}")
    except Exception as e:
        print(f"Error publishing message from local transcriber to Kafka topic '{topic}': {e}")
        raise

# --- Transcription Logic using Local Whisper Model ---
def transcribe_audio_chunk(audio_file_path: Path):
    transcript_text = None
    try:
        print(f"Transcribing {audio_file_path.name} using local Whisper model...")
        result = whisper_model.transcribe(str(audio_file_path))
        transcript_text = result["text"]
        
        if not transcript_text:
            print(f"No transcript text obtained for {audio_file_path}")
            return None

        print(f"Transcription completed: {transcript_text[:100]}...")
    except Exception as e:
        print(f"Error during transcription of {audio_file_path}: {e}")
        transcript_text = None
    
    return transcript_text


def process_claimed_chunk(claim_response: dict): # Takes the claim response from the bridge
    """
    Processes a chunk that has been successfully claimed from the bridge service.
    """
    chunk_id_to_process = claim_response['chunk_id']
    original_metadata = claim_response['metadata']
    claimed_by_id = claim_response['claimed_by'] # The ID the bridge assigned for this claim

    print(f"\n[{LOCAL_TRANSCRIBER_ID}] Processing claimed chunk: {chunk_id_to_process}")
    download_url = f"{BRIDGE_SERVICE_URL}/audio-chunks/{chunk_id_to_process}"
    
    temp_local_dir = Path(f"/tmp/local_transcription_chunks/{chunk_id_to_process}")
    temp_local_dir.mkdir(parents=True, exist_ok=True)
    local_audio_path = temp_local_dir / original_metadata['chunk_filename']

    transcription_successful = False
    try:
        print(f"[{LOCAL_TRANSCRIBER_ID}] Downloading from {download_url}")
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        
        with open(local_audio_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"[{LOCAL_TRANSCRIBER_ID}] Downloaded chunk to {local_audio_path}")

        transcript = transcribe_audio_chunk(local_audio_path)

        if transcript:
            result_message = {
                'video_id': original_metadata['video_id'],
                'chunk_id': original_metadata['chunk_id'],
                'start_time_sec': original_metadata['start_time_sec'],
                'end_time_sec': original_metadata['end_time_sec'],
                'transcript_text': transcript,
                'source_chunk_url': original_metadata['chunk_url']
            }
            publish_kafka_message(KAFKA_TOPIC_TRANSCRIPTION_RESULTS, result_message)
            transcription_successful = True
        else:
            print(f"[{LOCAL_TRANSCRIBER_ID}] No transcript generated for chunk {chunk_id_to_process}.")

    except requests.exceptions.RequestException as e:
        print(f"[{LOCAL_TRANSCRIBER_ID}] Error fetching chunk {chunk_id_to_process} from bridge: {e}")
    except Exception as e:
        print(f"[{LOCAL_TRANSCRIBER_ID}] Unexpected error during local chunk processing: {type(e).__name__}: {e}")
    finally:
        shutil.rmtree(temp_local_dir, ignore_errors=True)
        print(f"[{LOCAL_TRANSCRIBER_ID}] Cleaned up temporary directory: {temp_local_dir}")
        
        # --- IMPORTANT: Send confirmation back to bridge service ---
        # Send processed signal regardless of transcription success, but include claimed_by
        try:
            processed_url = f"{BRIDGE_SERVICE_URL}/chunks/{chunk_id_to_process}/processed"
            payload = {"claimed_by": claimed_by_id} # Pass the ID assigned by the bridge
            print(f"[{LOCAL_TRANSCRIBER_ID}] Sending processed signal to bridge for {chunk_id_to_process} (claimed_by: {claimed_by_id}): {processed_url}")
            response = requests.post(processed_url, json=payload) # Use json= for body
            response.raise_for_status()
            print(f"[{LOCAL_TRANSCRIBER_ID}] Bridge acknowledged chunk {chunk_id_to_process} as processed: {response.json()}")
        except requests.exceptions.RequestException as e:
            print(f"[{LOCAL_TRANSCRIBER_ID}] Error signaling bridge that chunk {chunk_id_to_process} was processed: {e}")
        except Exception as e:
            print(f"[{LOCAL_TRANSCRIBER_ID}] Unexpected error during processed signal: {type(e).__name__}: {e}")

# --- Main Local Listener Loop (Claim-based) ---
if __name__ == "__main__":
    print("Starting Local Audio Transcriber...")

    get_kafka_producer()

    print(f"[{LOCAL_TRANSCRIBER_ID}] Polling bridge service to claim chunks...")
    while True:
        try:
            # Attempt to claim a single chunk
            response = requests.post(f"{BRIDGE_SERVICE_URL}/pending-chunks/claim")
            response.raise_for_status() # Raises for 4XX/5XX responses

            claimed_chunk_info = response.json()
            print(f"[{LOCAL_TRANSCRIBER_ID}] Successfully claimed chunk: {claimed_chunk_info['chunk_id']}")
            
            # Process the claimed chunk
            process_claimed_chunk(claimed_chunk_info)

        except requests.exceptions.ConnectionError as e:
            print(f"[{LOCAL_TRANSCRIBER_ID}] Connection Error to bridge service {BRIDGE_SERVICE_URL}: {e}. Is port-forwarding active?")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # This is expected if no pending chunks are available
                print(f"[{LOCAL_TRANSCRIBER_ID}] No pending chunks available to claim. Waiting...")
            else:
                print(f"[{LOCAL_TRANSCRIBER_ID}] HTTP Error claiming chunk: {e} - {e.response.text}")
        except json.JSONDecodeError as e:
            print(f"[{LOCAL_TRANSCRIBER_ID}] Error decoding JSON response from bridge service: {e}")
        except Exception as e:
            print(f"[{LOCAL_TRANSCRIBER_ID}] An unexpected error occurred in claiming loop: {type(e).__name__}: {e}")
        
        time.sleep(5) # Wait before attempting to claim the next chunk