FROM nvidia/cuda:12.2.0-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    python3 python3-pip python3-venv \
    ghostscript \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/
RUN python3 -m pip install --upgrade pip \
    && pip install -r requirements.txt

COPY src/ /app/src/

ENTRYPOINT ["python3", "-m", "src.run_pipeline"]
