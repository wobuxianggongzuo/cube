FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api.py .
COPY crawler.py .

ENV PROJECT_ID=${PROJECT_ID}
ENV DATASET_ID=${DATASET_ID}
ENV TABLE_ID=${TABLE_ID}

CMD exec uvicorn api:app --host 0.0.0.0 --port ${PORT:-8080}