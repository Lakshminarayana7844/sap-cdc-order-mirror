FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Demo: generate CDC events, mirror them into Delta, print the result.
CMD ["bash", "scripts/run_demo.sh"]
