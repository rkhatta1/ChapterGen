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

def generate_chapters(video_id, all_segments, generation_config=None):
    """
    Generates chapters from a list of all transcript segments for a video.
    """
    if not model:
        print("Gemini model not initialized. Skipping chapter generation.")
        return []

    # --- Assemble Full Transcript with Sentence Timestamps ---
    full_transcript_for_prompt = ""
    for segment in all_segments:
        start = segment.get('start')
        end = segment.get('end')
        text = segment.get('text')
        full_transcript_for_prompt += f"[start: {start:.2f}s, end: {end:.2f}s] {text}\n"

    # --- Dynamic Prompt Generation ---
    creativity_style = generation_config.get('creativity', 'Neutral') if generation_config else 'Neutral'
    segmentation_threshold = generation_config.get('segmentation_threshold', 'Default') if generation_config else 'Default'

    style_instructions = {
        'GenZ': "Use trendy, casual, and slightly informal language. Include emojis where appropriate.",
        'Creative': "Use engaging, imaginative, and descriptive language.",
        'Neutral': "Use clear, concise, and objective language.",
        'Formal': "Use professional, structured, and formal language.",
        'Corporate': "Use business-oriented, professional, and polished language suitable for a corporate presentation."
    }
    style_instruction = style_instructions.get(creativity_style, style_instructions['Neutral'])

    threshold_instructions = {
        'Detailed': "Create many short, detailed chapters, identifying every minor shift in topic.",
        'Default': "Create a balanced number of chapters, focusing on the main talking points.",
        'Abstract': "Create only a few high-level chapters, summarizing the major themes of the video."
    }
    threshold_instruction = threshold_instructions.get(segmentation_threshold, threshold_instructions['Default'])

    final_timestamp = all_segments[-1]['end'] if all_segments else 0

    prompt = f"""
    You are an expert video editor tasked with creating semantic chapters for a YouTube video.
    Your goal is to identify the main topics in the video and create chapters that accurately reflect when each topic begins and ends.

    **Instructions:**
    1.  Analyze the complete, timestamped transcript provided below. Each line represents a sentence or phrase with its start and end time.
    2.  **Chapter Granularity:** {threshold_instruction}
    3.  **Chapter Title Style:** {style_instruction}
    4.  Identify the natural breakpoints in the conversation where the topic changes.
    5.  For each identified topic, create a chapter with a `start_time`, `end_time`, and a `title`.
    6.  The `start_time` of a chapter should be the `start` time of the first sentence of that topic.
    7.  The `end_time` of a chapter should be the `end` time of the last sentence of that topic.
    8.  The first chapter must start at 0.0 seconds.
    9.  The last chapter must end at the video's final timestamp: {final_timestamp:.2f} seconds.
    10. The output **MUST** be a valid JSON object containing a single key `chapters`, which is a list of chapter objects. Do not include any other text, explanations, or markdown formatting in your response.

    **Example JSON Output Format:**
    ```json
    {{
      "chapters": [
        {{
          "start_time": 0.0,
          "end_time": 33.5,
          "title": "The Initial Problem"
        }},
        {{
          "start_time": 33.5,
          "end_time": 92.1,
          "title": "Developing a Solution"
        }}
      ]
    }}
    ```

    **Timestamped Transcript:**
    {full_transcript_for_prompt}
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
        print(f"Successfully generated {len(chapters)} raw chapters from Gemini.")

        # --- Filter and Merge Chapters to meet minimum duration ---
        if not chapters:
            return []

        print("\n---- RAW CHAPTERS -----")
        print(json.dumps(chapters, indent=2))
        print("------------------------\n")

        # Set minimum duration based on user's preference
        segmentation_threshold = generation_config.get('segmentation_threshold', 'Default')
        min_duration_map = {
            'Detailed': 15, # Allow shorter chapters for detailed requests
            'Default': 20,
            'Abstract': 30 # Encourage longer chapters for abstract requests
        }
        MIN_CHAPTER_DURATION = min_duration_map.get(segmentation_threshold, 20)
        print(f"Using minimum chapter duration of {MIN_CHAPTER_DURATION} seconds based on threshold: '{segmentation_threshold}'")

        final_chapters = []
        if not chapters:
            return []

        # Add the first chapter unconditionally, ensuring it starts at 0.0
        first_chapter = chapters[0]
        first_chapter['start_time'] = 0.0
        final_chapters.append(first_chapter)

        for i in range(1, len(chapters)):
            current_chapter = chapters[i]
            last_final_chapter = final_chapters[-1]

            # Calculate duration of the current chapter and its gap from the previous valid one
            duration = current_chapter['end_time'] - current_chapter['start_time']
            gap = current_chapter['start_time'] - last_final_chapter['start_time']

            if duration >= MIN_CHAPTER_DURATION and gap >= MIN_CHAPTER_DURATION:
                # This chapter is valid. First, close the gap with the previous chapter.
                last_final_chapter['end_time'] = current_chapter['start_time']
                final_chapters.append(current_chapter)
            else:
                # This chapter is too short or too close. Merge it with the previous one.
                last_final_chapter['end_time'] = current_chapter['end_time']
                print(f"Merging short or close chapter \"{current_chapter.get('title', 'Untitled')}\" into \"{last_final_chapter.get('title', 'Untitled')}\"")

        print(f"Filtered and merged chapters. Final count: {len(final_chapters)}")

        print("\n--- FINAL PROCESSED CHAPTERS ---")
        print(json.dumps(final_chapters, indent=2))
        print("------------------------------\n")

        return final_chapters

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
                transcription_chunks = video_transcripts.pop(video_id, [])
                del last_received_time[video_id]

                if transcription_chunks:
                    # Sort chunks by their original start time
                    transcription_chunks.sort(key=lambda x: x['start_time_sec'])
                    
                    # --- Assemble all segments from all chunks, making timestamps absolute ---
                    all_segments = []
                    for chunk in transcription_chunks:
                        chunk_start_time = chunk.get('start_time_sec', 0)
                        for segment in chunk.get('segments', []):
                            # Convert relative segment timestamps to absolute video timestamps
                            segment['start'] += chunk_start_time
                            segment['end'] += chunk_start_time
                            all_segments.append(segment)

                    if not all_segments:
                        print(f"No segments found for video {video_id}. Skipping chapter generation.")
                        continue

                    # Extract generation_config from the first chunk
                    generation_config = transcription_chunks[0].get('generation_config', {})


                    user_id = transcription_chunks[0].get('user_id')
                    chapters = generate_chapters(video_id, all_segments, generation_config)
                    if chapters:
                        message = {
                            "video_id": video_id,
                            "chapters": chapters,
                            "user_id": user_id,
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