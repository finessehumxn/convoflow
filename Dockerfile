FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY convoflow.py adapters.py api.py jobs.py ./
COPY migrations ./migrations
USER nobody
# ECS passes its own command. Elsewhere (Railway) CONVOFLOW_ROLE picks the process.
CMD ["sh", "-c", "case \"${CONVOFLOW_ROLE:-api}\" in worker) exec python -c 'import convoflow; convoflow.run_worker()' ;; *) python jobs.py migrate && python jobs.py seed-key && exec python api.py ;; esac"]
