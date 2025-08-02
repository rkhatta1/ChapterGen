# Dockerfile for Ingestion Service
FROM python:3.9-slim-bullseye

# Install system dependencies required for yt-dlp and ffmpeg
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3-pip \
    python3-setuptools \
    ffmpeg \
    # yt-dlp \
    && \
    rm -rf /var/lib/apt/lists/* \
    && \
    apt-get clean

WORKDIR /app

# Copy requirements.txt and install Python dependencies
COPY requirements-ingest.txt .
RUN pip install --no-cache-dir -r requirements-ingest.txt

# Copy your application code
COPY app.py .

# CMD for FastAPI with Uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]