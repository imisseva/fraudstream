FROM python:3.11-slim

WORKDIR /app

# Copy requirements trước để tận dụng Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code simulator
COPY simulator/ ./simulator/

# Entry point
CMD ["python", "-m", "simulator.main"]

