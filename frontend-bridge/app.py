import asyncio
import json
import websockets
import httpx
from aiokafka import AIOKafkaConsumer

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
        print(f"Profile from Google: {profile_data}")
        return profile_data
    except httpx.RequestError as e:
        print(f"Error fetching user profile: {e}")
        return None

async def get_or_create_user(profile):
    """Creates a user in the database-service if they don't exist."""
    try:
        email = profile.get('email')
        name = profile.get('name')
        if not email or not name:
            return None

        async with httpx.AsyncClient() as client:
            # Check if user exists
            response = await client.get(f"{DATABASE_SERVICE_URL}/users/by-email/{email}")
            print(f"Database service response (get user): Status {response.status_code}, Body: {response.text}")
            if response.status_code == 200:
                return response.json()
            
            # If not, create the user
            response = await client.post(
                f"{DATABASE_SERVICE_URL}/users/",
                json={"email": email, "name": name}
            )
            print(f"Database service response (create user): Status {response.status_code}, Body: {response.text}")
        response.raise_for_status()
        return response.json()
    except httpx.RequestError as e:
        print(f"Error creating or getting user: {e}")
        return None

async def kafka_consumer_task():
    """Consumes messages from Kafka and sends them to the correct user."""
    consumer = AIOKafkaConsumer(
        'chapter-results',
        bootstrap_servers='my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
        group_id="frontend_bridge_group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(f"Consumed message from Kafka: {msg.value}")
            user_id = msg.value.get("user_id")
            if user_id and user_id in connected_clients:
                websocket = connected_clients[user_id]
                message_to_send = json.dumps(msg.value)
                await websocket.send(message_to_send)
    except Exception as e:
        print(f"Kafka consumer error: {e}")
    finally:
        await consumer.stop()

async def register(websocket):
    """Registers a new client, authenticates them, and keeps the connection open."""
    user_id = None
    try:
        # 1. Wait for the initial authentication message
        auth_message = await websocket.recv()
        auth_data = json.loads(auth_message)
        access_token = auth_data.get('access_token')

        if not access_token:
            await websocket.close(code=1008, reason="Access token not provided")
            return

        # 2. Get user profile and create/get user from DB
        profile = await get_user_profile(access_token)
        if not profile:
            await websocket.close(code=1011, reason="Invalid access token or profile fetch failed")
            return
        
        user = await get_or_create_user(profile)
        if not user or 'id' not in user:
            await websocket.close(code=1011, reason="Could not create or get user from DB or user object missing ID")
            return
        
        user_id = user['id']
        connected_clients[user_id] = websocket
        print(f"Client connected and authenticated for user_id: {user_id}. Total clients: {len(connected_clients)}")

        # 3. Keep the connection open
        async for message in websocket:
            pass # We don't expect more messages from the client

    except websockets.exceptions.ConnectionClosed:
        print(f"Client connection closed for user_id: {user_id}")
    finally:
        if user_id and user_id in connected_clients:
            del connected_clients[user_id]
            print(f"Client disconnected for user_id: {user_id}. Total clients: {len(connected_clients)}")

async def main():
    kafka_task = asyncio.create_task(kafka_consumer_task())
    async with websockets.serve(register, "0.0.0.0", 8765):
        print("WebSocket server started on ws://0.0.0.0:8765")
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server shutting down.")
