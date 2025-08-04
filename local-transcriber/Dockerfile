# Use an official NVIDIA CUDA base image to ensure GPU compatibility
FROM nvidia/cuda:12.1.0-base-ubuntu22.04

# Avoid prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install Python, pip, and essential audio/video processing tools
RUN apt-get update && apt-get install -y \
    python3.10 \
    python3-pip \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the transcriber application code
COPY app.py .

# This is the command that Cloud Run will execute when a job starts
CMD ["python3", "app.py"]