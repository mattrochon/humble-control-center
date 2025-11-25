FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# Allow building either from repo root (standard) or from docker/ (manual docker build).
# If the project files are in the parent, copy them; else copy current dir.
ARG APP_SRC=.
COPY ${APP_SRC} /app

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e .

EXPOSE 8000

# Persist DB/state and downloads
VOLUME ["/app/data", "/library"]

ENTRYPOINT ["python", "-m", "humblebundle_downloader.ui_server"]
