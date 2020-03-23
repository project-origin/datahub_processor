FROM python:3.7-slim

LABEL Name=measurement_transactions_processor Version=0.0.1

WORKDIR /app

COPY ./requirements.txt requirements.txt

RUN apt-get update &&\
    apt-get install \
        pkg-config \
        libsecp256k1-dev \
        gcc \
        libzmq3-dev -y &&\
    python3 -m pip install -r requirements.txt &&\
    rm requirements.txt

COPY ./src /app

ENTRYPOINT ["python3", "main.py"]
