# Dockerfile — Bolt KV Store node
#
# Build:  docker build -t bolt-kv .
# Run:    docker run -p 6379:6379 bolt-kv --id A --port 6379

FROM python:3.12-slim

WORKDIR /app

# Copy all source files
COPY engine.py wal.py server.py raft.py raft_node.py ring.py lru_cache.py ./

# Create WAL directory
RUN mkdir -p /data/wal

# Expose client port
EXPOSE 6379

# Health check — ping the server every 30s
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD python -c "import socket; s=socket.socket(); s.connect(('localhost', int(__import__('os').environ.get('PORT','6379')))); s.sendall(b'PING\n'); r=s.recv(64); s.close(); exit(0 if b'PONG' in r else 1)"

ENTRYPOINT ["python"]
CMD ["http_server.py"]
