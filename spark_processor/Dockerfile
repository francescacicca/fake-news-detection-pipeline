FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk procps curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME="/usr/lib/jvm/default-java"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "processor.py"]