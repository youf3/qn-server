# syntax=docker/dockerfile:1.4
FROM python:3.10-slim

ARG QN_SERVER_REF="develop"
ARG QN_MQ_REF="develop"

# Install OS deps in one layer
RUN apt-get update && \
    apt-get install --yes --no-install-recommends git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Upgrade pip tooling
RUN pip install --upgrade pip setuptools wheel

# Pre-install some known dependency constraints to avoid backtracking
RUN pip install "pluggy<1.0.0" "readme-renderer>=42.0" "docutils>=0.21.2"

# Clone repos with secrets (BuildKit secret mount)
RUN --mount=type=secret,id=GH_TOKEN \
    GH_TOKEN=$(cat /run/secrets/GH_TOKEN) && \
    git clone https://${GH_TOKEN}@github.com/quant-net/quant-net-mq -b ${QN_MQ_REF} && \
    git clone https://${GH_TOKEN}@github.com/quant-net/quant-net-plugins --branch develop && \
    git clone https://${GH_TOKEN}@github.com/quant-net/quant-net-server -b ${QN_SERVER_REF}

# Install Python dependencies with pip cache mount
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install ./quant-net-mq && \
    pip install ./quant-net-server