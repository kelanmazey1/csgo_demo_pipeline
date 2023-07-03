FROM debian:buster-slim
ENV PYTHONUNBUFFERED=1
RUN apt-get update && \
    apt-get install -y python3 \
    python3-pip
    
RUN pip3 install --no-cache --upgrade pip setuptools

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

RUN mkdir -p ./demo_dump/rar_files/ ./demo_dump/demo_files/


COPY ./dl_unzip.py ./

ENTRYPOINT [ "bash" ]