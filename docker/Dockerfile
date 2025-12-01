FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

# Allow building either from repo root (standard) or from docker/ (manual docker build).
# If the project files are in the parent, copy them; else copy current dir.
ARG APP_SRC=.
COPY ${APP_SRC} /app

ENV DEBIAN_FRONTEND=noninteractive
# Install Node + Yarn for UI build (Node 22 LTS) via official tarball to avoid apt warnings.
ARG NODE_VERSION=22.12.0
RUN apt-get update -yq \
    && apt-get install -yq --no-install-recommends curl \
    && curl -fsSL https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz -o /tmp/node.tar.xz \
    && mkdir -p /usr/local/node \
    && tar -xJf /tmp/node.tar.xz -C /usr/local/node --strip-components=1 \
    && rm /tmp/node.tar.xz \
    && ln -s /usr/local/node/bin/corepack /usr/local/bin/corepack \
    && ln -s /usr/local/node/bin/node /usr/local/bin/node \
    && ln -s /usr/local/node/bin/npm /usr/local/bin/npm \
    && ln -s /usr/local/node/bin/npx /usr/local/bin/npx \
    && corepack enable \
    && /usr/local/node/bin/corepack prepare yarn@1.22.22 --activate \
    && rm -rf /var/lib/apt/lists/*
ENV PATH="/usr/local/node/bin:${PATH}"

# Install Python deps and build React UI.
# Upgrade pip to latest and ignore root warning via env.
ENV PIP_ROOT_USER_ACTION=ignore
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e . \
    && yarn --cwd react-ui install \
    && yarn --cwd react-ui build

EXPOSE 8000

# Persist DB/state and downloads
VOLUME ["/app/data", "/library"]

ENTRYPOINT ["python", "-m", "humblebundle_downloader.ui_server"]
