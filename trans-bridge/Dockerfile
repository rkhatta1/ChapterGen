# Dockerfile for transcription-bridge-service
FROM python:3.9-slim

WORKDIR /app

COPY requirements-bridge.txt .
RUN pip install --no-cache-dir -r requirements-bridge.txt

COPY app.py .

# CORRECTED: Run the python script directly
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]