FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Create data directory
RUN mkdir -p /data

# Default port (Railway will override via PORT env var)
ENV PORT=8000

# Run the application using shell to expand PORT variable
CMD exec uvicorn main:app --host 0.0.0.0 --port $PORT
