import asyncio
import json
import websockets
from aiokafka import AIOKafkaConsumer

# In-memory set of connected WebSocket clients
connected_clients = set()

async def kafka_consumer_task():
    """Consumes messages from Kafka and broadcasts them to WebSocket clients."""
    consumer = AIOKafkaConsumer(
        'chapter-results',
        bootstrap_servers='my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
        group_id="frontend_bridge_group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        # In a real K8s setup, you might need to handle DNS resolution differently
        # or pass the bootstrap server address as an environment variable.
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(f"Consumed message from Kafka: {msg.value}")
            if connected_clients:
                # Broadcast the message to all connected clients
                message_to_send = json.dumps(msg.value)
                # Use asyncio.gather to send messages concurrently
                await asyncio.gather(*[client.send(message_to_send) for client in connected_clients])
    except Exception as e:
        print(f"Kafka consumer error: {e}")
    finally:
        await consumer.stop()

async def register(websocket):
    """Registers a new client and keeps the connection open."""
    connected_clients.add(websocket)
    print(f"New client connected. Total clients: {len(connected_clients)}")
    try:
        # Keep the connection open and listen for messages (though we don't expect any from client)
        async for message in websocket:
            pass
    except websockets.exceptions.ConnectionClosed:
        print("Client connection closed.")
    finally:
        connected_clients.remove(websocket)
        print(f"Client disconnected. Total clients: {len(connected_clients)}")

async def main():
    """Starts both the Kafka consumer and the WebSocket server."""
    # Start the Kafka consumer task in the background
    kafka_task = asyncio.create_task(kafka_consumer_task())

    # Start the WebSocket server
    async with websockets.serve(register, "0.0.0.0", 8765):
        print("WebSocket server started on ws://0.0.0.0:8765")
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server shutting down.")