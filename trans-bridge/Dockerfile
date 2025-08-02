# Dockerfile for Audio Transcription Worker (Local Whisper)
FROM python:3.9-slim-bullseye

# Install system dependencies required for Whisper (ffmpeg, build tools for some python packages)
# libsndfile1 is often required by torchaudio on Debian-based systems.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    build-essential \
    libsndfile1 \
    libgomp1 \
    && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements.txt and install Python dependencies
# This will pull in openai-whisper, torch, torchaudio, etc.
# Note: For CPU, PyTorch can be very large. Consider multi-stage build or slim base image if space is critical.
COPY requirements-bridge.txt .

RUN pip install --no-cache-dir -r requirements-bridge.txt

# Copy your application code
COPY app.py .

# This CMD runs your worker loop continuously
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]