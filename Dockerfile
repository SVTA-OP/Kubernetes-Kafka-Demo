# Use a lightweight Python base image
FROM python:3.11-slim

WORKDIR /app

# Install our dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- THE MISSING LINE ---
# Copy all the python scripts and html into the image
COPY . .
# ------------------------

# Start the FastAPI server (without --reload for K8s)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
