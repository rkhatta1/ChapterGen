import asyncio
import json
import websockets
import httpx
from aiokafka import AIOKafkaConsumer
import traceback # Import traceback to print full errors

print("--- [INIT] Starting frontend-bridge application ---")

# In-memory dictionary to map user_id to WebSocket client
connected_clients = {}
DATABASE_SERVICE_URL = "http://database-service-service:8008"

async def get_user_profile(access_token):
    """Fetches user profile from Google using the access token."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://www.googleapis.com/oauth2/v1/userinfo",
                headers={"Authorization": f"Bearer {access_token}"}
            )
        response.raise_for_status() # Raises an exception for bad responses (4xx or 5xx)
        profile_data = response.json()
        return profile_data
    except httpx.RequestError as e:
        print(f"--- [ERROR] Failed to fetch user profile from Google: {e} ---")
        return None

async def get_or_create_user(profile):
    """Creates a user in the database-service if they don't exist."""
    try:
        email = profile.get('email')
        name = profile.get('name')
        if not email or not name:
            print("--- [ERROR] Profile is missing email or name ---")
            return None

        async with httpx.AsyncClient() as client:
            # Check if user exists
            response = await client.get(f"{DATABASE_SERVICE_URL}/users/by-email/{email}")
            if response.status_code == 200:
                return response.json()
            
            # If not, create the user
            response = await client.post(
                f"{DATABASE_SERVICE_URL}/users/",
                json={"email": email, "name": name}
            )
        response.raise_for_status()
        return response.json()
    except httpx.RequestError as e:
        print(f"--- [ERROR] Failed during database operation: {e} ---")
        return None

async def kafka_consumer_task():
    """Consumes messages from Kafka and sends them to the correct user."""
    consumer = AIOKafkaConsumer(
        'chapter-results',
        bootstrap_servers='my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
        group_id="frontend_bridge_group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000
    )
    await consumer.start()
    print("--- [KAFKA] Consumer started successfully. Listening for messages... ---")
    try:
        async for msg in consumer:
            print(f"--- [KAFKA] Consumed message: {msg.value} ---")
            user_id = msg.value.get("user_id")
            if user_id and user_id in connected_clients:
                websocket = connected_clients[user_id]
                # Notify the frontend that chapters are ready for processing
                message_to_send = json.dumps({
                    "type": "chapters_ready",
                    "data": msg.value
                })
                await websocket.send(message_to_send)
    except Exception as e:
        print(f"--- [CRITICAL KAFKA FAIL] Kafka consumer task failed: {e} ---")
        traceback.print_exc()
    finally:
        await consumer.stop()


async def register(websocket):
    """Registers a new client, authenticates them, and handles all communication."""
    user_id = None
    try:
        # --- Authentication Handshake ---
        auth_message = await websocket.recv()
        auth_data = json.loads(auth_message)
        access_token = auth_data.get('access_token')

        if not access_token:
            await websocket.close(code=1008, reason="Access token not provided")
            return

        profile = await get_user_profile(access_token)
        if not profile:
            await websocket.close(code=1011, reason="Invalid access token or profile fetch failed")
            return

        user = await get_or_create_user(profile)
        if not user or 'id' not in user:
            await websocket.close(code=1011, reason="Could not create or get user from DB")
            return

        user_id = user['id']
        connected_clients[user_id] = websocket
        print(f"--- [WS] Client authenticated for user_id: {user_id}. Connection stable. ---")

        # --- Listen for Messages from the Client ---
        async for message in websocket:
            try:
                data = json.loads(message)
                # Handle the status update message from the frontend
                if data.get("type") == "status_update":
                    video_id = data.get("video_id")
                    status = data.get("status")
                    if video_id and status:
                        async with httpx.AsyncClient() as client:
                            response = await client.put(
                                f"{DATABASE_SERVICE_URL}/jobs/{video_id}/status",
                                json={"status": status}
                            )
                            response.raise_for_status()
                        
                        print(f"--- [DB] Updated status for video_id {video_id} to {status} ---")
            except json.JSONDecodeError:
                print(f"--- [WS-ERROR] Received invalid JSON from user_id: {user_id} ---")
            except Exception as e:
                print(f"--- [WS-ERROR] Error processing message from user_id {user_id}: {e} ---")

    except websockets.exceptions.ConnectionClosed:
        print(f"--- [WS] Connection closed for user_id: {user_id} ---")
    except Exception as e:
        print(f"--- [CRITICAL WS FAIL] An unexpected error occurred in the register function: {e} ---")
        traceback.print_exc()
    finally:
        if user_id and user_id in connected_clients:
            del connected_clients[user_id]
        print(f"--- [WS] Client disconnected for user_id: {user_id}. Total clients: {len(connected_clients)} ---")


async def main():
    kafka_task = asyncio.create_task(kafka_consumer_task())
    async with websockets.serve(register, "0.0.0.0", 8765):
        print("--- [MAIN] WebSocket server started on ws://0.0.0.0:8765 ---")
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("--- [EXIT] Server shutting down. ---")