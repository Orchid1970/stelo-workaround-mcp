FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Create data directory
RUN mkdir -p /data

# Expose port
EXPOSE 8000

# Run the application - use PORT env var for Railway compatibility
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
