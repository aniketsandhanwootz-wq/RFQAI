# service/Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System deps for some parsers (pymupdf generally ok; pdfplumber may need these)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    poppler-utils \
  && rm -rf /var/lib/apt/lists/*

COPY service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r /app/requirements.txt
# Copy whole repo (needed because you read contracts from packages/)
COPY . /app

EXPOSE 8000

# Render-friendly: bind to $PORT
CMD ["bash", "-lc", "uvicorn service.app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]