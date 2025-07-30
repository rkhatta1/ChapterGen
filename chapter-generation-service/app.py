# chapter-generation-service/app.py
import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import google.generativeai as genai

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092')
KAFKA_TOPIC_TRANSCRIPTION_RESULTS = os.environ.get('KAFKA_TOPIC_TRANSCRIPTION_RESULTS', 'transcription-results')
KAFKA_TOPIC_CHAPTER_RESULTS = os.environ.get('KAFKA_TOPIC_CHAPTER_RESULTS', 'chapter-results')
KAFKA_CONSUMER_GROUP_ID = os.environ.get('KAFKA_CONSUMER_GROUP_ID', 'chapter-generation-group')
VIDEO_COMPLETION_TIMEOUT = int(os.environ.get('VIDEO_COMPLETION_TIMEOUT', '30')) # seconds
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')

# --- Gemini API Setup ---
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.0-flash')
else:
    model = None
    print("Warning: GEMINI_API_KEY not found. Chapter generation will be disabled.")


# --- Kafka Consumer Setup ---
consumer = None
def get_kafka_consumer():
    global consumer
    if consumer is None:
        print(f"Initializing Kafka Consumer for topic '{KAFKA_TOPIC_TRANSCRIPTION_RESULTS}', group '{KAFKA_CONSUMER_GROUP_ID}'")
        consumer = KafkaConsumer(
            KAFKA_TOPIC_TRANSCRIPTION_RESULTS,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=KAFKA_CONSUMER_GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    return consumer

# --- Kafka Producer Setup (for results) ---
producer = None
def get_kafka_producer():
    global producer
    if producer is None:
        print(f"Initializing Kafka Producer for results topic: {KAFKA_TOPIC_CHAPTER_RESULTS}")
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
        print(f"Published Kafka message to topic '{topic}': {json.dumps(message, indent=2)}")
    except Exception as e:
        print(f"Error publishing message to Kafka topic '{topic}': {e}")
        raise

def generate_chapters(transcripts):
    """
    Generates chapters from a list of transcripts using the Gemini API.
    """
    if not model:
        print("Gemini model not initialized. Skipping chapter generation.")
        return []

    # Combine transcript text and add timestamps for context
    full_transcript_with_times = ""
    for t in transcripts:
        start = t['start_time_sec']
        text = t['transcript_text']
        full_transcript_with_times += f"[start_time: {start:.2f}s] {text}\n"

    prompt = f"""
    You are an expert at creating YouTube video chapters.
    Based on the following transcript with timestamps, please generate a concise list of chapters.
    Each chapter must have a "start_time" (in seconds), an "end_time" (in seconds), and a short, descriptive "title".
    The start_time of the first chapter must be the start_time of the first transcript segment.
    The end_time of the last chapter must be the end_time of the last transcript segment.
    The output must be a valid JSON object containing a single key "chapters", which is a list of chapter objects. Do not include any other text or explanations in your response.

    Example format:
    {{
      "chapters": [
        {{
          "start_time": 0.0,
          "end_time": 58.5,
          "title": "Introduction to the System"
        }},
        {{
          "start_time": 58.5,
          "end_time": 125.2,
          "title": "Explaining the Ingestion Service"
        }}
      ]
    }}

    Transcript:
    {full_transcript_with_times}
    """

    try:
        print("Generating chapters with Gemini...")
        response = model.generate_content(prompt)
        
        # More robust JSON extraction
        text_response = response.text
        start_index = text_response.find('{')
        end_index = text_response.rfind('}') + 1
        
        if start_index == -1 or end_index == 0:
            raise ValueError("Could not find a JSON object in the Gemini response.")
            
        json_string = text_response[start_index:end_index]
        
        chapters = json.loads(json_string)['chapters']
        print(f"Successfully generated {len(chapters)} chapters.")
        return chapters
    except Exception as e:
        print(f"Error generating chapters with Gemini: {e}")
        print(f"Gemini response was: {response.text if 'response' in locals() else 'N/A'}")
        return []

# --- Main Worker Loop ---
def start_chapter_generation_worker():
    consumer = get_kafka_consumer()
    producer = get_kafka_producer()
    print("Starting chapter generation worker loop...")

    video_transcripts = {}
    last_received_time = {}

    while True:
        try:
            # Process incoming messages
            messages = consumer.poll(timeout_ms=1000, max_records=10)
            if messages:
                for tp, consumer_records in messages.items():
                    for message in consumer_records:
                        transcription_result = message.value
                        video_id = transcription_result.get('video_id')
                        
                        if video_id:
                            if video_id not in video_transcripts:
                                video_transcripts[video_id] = []
                            video_transcripts[video_id].append(transcription_result)
                            last_received_time[video_id] = time.time()
                            print(f"Received and stored transcript for video_id: {video_id} (start: {transcription_result.get('start_time_sec')})")

            # Check for completed videos
            completed_videos = []
            for video_id, last_time in last_received_time.items():
                if time.time() - last_time > VIDEO_COMPLETION_TIMEOUT:
                    completed_videos.append(video_id)
            
            for video_id in completed_videos:
                print(f"Video {video_id} timed out. Processing for chapter generation.")
                transcripts = video_transcripts.pop(video_id, [])
                del last_received_time[video_id]

                if transcripts:
                    # Sort transcripts by start time
                    transcripts.sort(key=lambda x: x['start_time_sec'])
                    chapters = generate_chapters(transcripts)
                    if chapters:
                        message = {
                            "video_id": video_id,
                            "chapters": chapters
                        }
                        publish_kafka_message(KAFKA_TOPIC_CHAPTER_RESULTS, message)

        except KafkaError as e:
            print(f"CRITICAL KAFKA ERROR in chapter generation worker main loop: {e}")
            time.sleep(5)
        except Exception as e:
            print(f"CRITICAL GENERAL ERROR in chapter generation worker main loop: {type(e).__name__}: {e}")
            time.sleep(5)

if __name__ == "__main__":
    print("Starting Chapter Generation Service...")
    start_chapter_generation_worker()