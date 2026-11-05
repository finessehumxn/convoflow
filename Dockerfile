FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY schema.sql convoflow.py adapters.py api.py ./
USER nobody
CMD ["python", "api.py"]
