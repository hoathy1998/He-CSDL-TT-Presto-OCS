FROM python:3.12-slim

# iproute2: tc (giới hạn băng thông mạng của node OCS); brotli: giải nén dữ liệu Laghos
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 brotli curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
