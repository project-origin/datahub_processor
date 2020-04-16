FROM python:3.7-slim

WORKDIR /root/app

COPY ./requirements.txt requirements.txt

RUN apt-get update &&\
    apt-get install \
        pkg-config \
        libsecp256k1-dev \
        gcc \
        libzmq3-dev -y &&\
    python3 -m pip install -r requirements.txt &&\
    rm requirements.txt

COPY ./src /root/app/libs
COPY ./src/origin_processor.py /root/app/origin_processor.py

ENTRYPOINT ["python3", "origin_processor.py"]
