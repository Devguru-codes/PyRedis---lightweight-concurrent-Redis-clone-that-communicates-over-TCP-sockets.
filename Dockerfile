FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir -e .

EXPOSE 6380

CMD ["python", "-m", "pyredis", "--host", "0.0.0.0", "--port", "6380"]

