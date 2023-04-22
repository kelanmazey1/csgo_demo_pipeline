# Downloads match from parquet file runs parse container then moves on to next
FROM debian:buster-slim

RUN apt-get update && \
    apt-get install -y python3 \
    python3-pip

RUN pip3 install --no-cache --upgrade pip setuptools

WORKDIR /setup/
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

RUN rm -rf /setup

WORKDIR /demo_dump/process

CMD ["bash"]