# Simple Dockerfile for running monitor_agent.py on Cloud Run
FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Default command: run the monitor once and exit
CMD ["python", "monitor_agent.py"]
