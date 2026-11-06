FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY convoflow.py adapters.py api.py jobs.py ./
COPY migrations ./migrations
USER nobody
CMD ["python", "api.py"]
