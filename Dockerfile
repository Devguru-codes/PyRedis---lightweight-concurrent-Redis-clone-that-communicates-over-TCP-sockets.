FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYREDIS_CONFIG=/app/pyredis.toml

COPY . .

RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir -e .
RUN mkdir -p /app/data /app/benchmarks

EXPOSE 6380 9101

CMD ["sh", "-c", "python -m pyredis --config ${PYREDIS_CONFIG} --host 0.0.0.0 --port 6380"]
