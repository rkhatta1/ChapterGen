import requests
import os
import json
from pathlib import Path
import shutil
import whisper
import torch

# --- Global model loading ---
print("Loading Whisper model...")
try:
    whisper_model = whisper.load_model(os.environ.get("WHISPER_MODEL_SIZE", "small"), device="cuda")
    print(f"Whisper model loaded successfully.")
except Exception as e:
    print(f"Failed to load Whisper model: {e}")
    exit(1)

# --- Configuration is now from Environment Variables ---
BRIDGE_SERVICE_URL = os.environ.get("BRIDGE_SERVICE_URL")
CHUNK_DATA_JSON = os.environ.get("CHUNK_DATA_JSON")

# --- Transcription Logic using Local Whisper Model ---
def transcribe_audio_chunk(audio_file_path: Path):
    """Transcribes the audio file and returns sentence-level segments."""
    try:
        print(f"Transcribing {audio_file_path.name} using local Whisper model for sentence-level segments...")
        # By not specifying word_timestamps, we get sentence-level segments by default.
        with torch.no_grad():
            result = whisper_model.transcribe(str(audio_file_path))
        
        if not result or "segments" not in result:
            print(f"No segments obtained for {audio_file_path}")
            return None

        # We only need the 'text', 'start', and 'end' keys for each segment.
        # This simplifies the data sent over Kafka.
        simplified_segments = [
            {"text": seg["text"], "start": seg["start"], "end": seg["end"]}
            for seg in result["segments"]
        ]

        print(f"Transcription completed for {audio_file_path.name}. Segments found: {len(simplified_segments)}")
        return simplified_segments

    except Exception as e:
        print(f"Error during transcription of {audio_file_path}: {e}")
        return None


def process_chunk(chunk_data: dict):
    # --- CORRECTED CODE ---
    # The chunk_data itself is the metadata. No need for a sub-key.
    chunk_id = chunk_data.get('chunk_id')
    print(f"Processing claimed chunk: {chunk_id}")
    download_url = f"{BRIDGE_SERVICE_URL}/audio-chunks/{chunk_id}"

    temp_dir = Path(f"/tmp/{chunk_id}")
    temp_dir.mkdir(parents=True, exist_ok=True)
    local_audio_path = temp_dir / chunk_data.get('chunk_filename')

    try:
        print(f"Downloading from {download_url}")
        response = requests.get(download_url, stream=True)
        response.raise_for_status()

        with open(local_audio_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded chunk to {local_audio_path}")

        transcript_segments = transcribe_audio_chunk(local_audio_path)

        if transcript_segments:
            # The result message is the original chunk_data with the segments added.
            result_message = chunk_data.copy()
            result_message['segments'] = transcript_segments

            submit_url = f"{BRIDGE_SERVICE_URL}/chunks/{chunk_id}/transcribed"
            print(f"Submitting results to {submit_url}")
            response = requests.post(submit_url, json=result_message)
            response.raise_for_status()
            print(f"Bridge acknowledged results for chunk {chunk_id}")
        else:
            print(f"No transcript generated for chunk {chunk_id}.")

    except requests.exceptions.RequestException as e:
        print(f"Error during processing: {e}")
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        print(f"Cleaned up temporary directory.")

# --- Main execution block (no loop) ---
if __name__ == "__main__":
    if not BRIDGE_SERVICE_URL or not CHUNK_DATA_JSON:
        print("Error: BRIDGE_SERVICE_URL and CHUNK_DATA_JSON must be set as environment variables.")
        exit(1)
        
    print("Starting Cloud Run transcription task...")
    try:
        chunk_info = json.loads(CHUNK_DATA_JSON)
        process_chunk(chunk_info)
        print("Transcription task completed successfully.")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON provided in CHUNK_DATA_JSON.")
    except Exception as e:
        print(f"A critical error occurred: {e}")