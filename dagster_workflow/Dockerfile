FROM debian:buster-slim
ENV PYTHONUNBUFFERED=1
RUN apt-get update && \
    apt-get install -y python3.9 \
    python3-pip \
    wget \
    unzip

# Get chrome chromedriver for Selenium
ARG CHROME_VERSION="113.0.5672.92-1"
# VERSION installed locally 114.0.5735.90-1

RUN wget --no-verbose -O /tmp/chrome.deb https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_VERSION}_amd64.deb \
  && apt install -y /tmp/chrome.deb \
  && rm /tmp/chrome.deb

RUN pip3 install --no-cache --upgrade pip setuptools

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

RUN mkdir -p ./demo_dump/rar_files/ ./demo_dump/demo_files

COPY ./dl_unzip.py ./

ENTRYPOINT [ "bash" ]