# Dockerfile for chapter-generation-service
FROM python:3.9-slim

WORKDIR /app

COPY requirements-chapters.txt .
RUN pip install --no-cache-dir -r requirements-chapters.txt

COPY . .

CMD ["python3", "app.py"]
