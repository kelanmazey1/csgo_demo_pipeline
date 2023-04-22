FROM python:3.9.9-slim-buster

ENV PYTHONUNBUFFERED=1

WORKDIR /setup
RUN apt-get update 
# Install wget for downloading tar.gz
RUN apt-get install -y wget

# Install go for use with awpy - awpy is a python wrapper for go library so both need to be installed
RUN wget https://go.dev/dl/go1.20.1.linux-amd64.tar.gz 
RUN tar -C /usr/local -xzf go1.20.1.linux-amd64.tar.gz

ENV PATH="${PATH}:/usr/local/go/bin"
# Remove tar after install
RUN rm -rf /setup

RUN pip3 install --no-cache --upgrade pip setuptools

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

WORKDIR /
ENTRYPOINT [ "bash" ]