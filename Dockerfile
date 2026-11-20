FROM python:3.11-slim

WORKDIR /srv/app

# psycopg[binary] needs no system libpq; keep the image minimal.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8080
# Single worker: the advisory lock plus unique ledger constraint already
# provide cross-process safety, but one worker keeps the demo deterministic.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
