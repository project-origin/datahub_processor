FROM python:3.7-slim

WORKDIR /root/app

COPY Pipfile /root/app
COPY Pipfile.lock /root/app

RUN apt-get update &&\
    apt-get install \
        pkg-config \
        libsecp256k1-dev \
        gcc \
        libzmq3-dev -y &&\
    pip3 install --upgrade twine wheel setuptools pip pipenv &&\
    pipenv sync

COPY ./src /root/app

ENTRYPOINT ["pipenv", "run", "main.py"]
