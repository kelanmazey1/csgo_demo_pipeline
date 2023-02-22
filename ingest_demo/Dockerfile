FROM ubuntu:22.04

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

# Install python and packages
RUN apt-get install -y software-properties-common gcc && \
    add-apt-repository -y ppa:deadsnakes/ppa

RUN apt-get update && apt-get install -y python3.10 python3-distutils python3-pip python3-apt

WORKDIR /reqs
COPY requirments.txt /reqs/requirements.txt
RUN pip3 install -r requirements.txt

ENTRYPOINT [ "bash" ]