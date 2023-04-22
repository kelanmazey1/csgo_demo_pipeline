FROM debian:buster-slim
ENV PYTHONUNBUFFERED=1
RUN apt-get update && \
    apt-get install -y python3 \
    python3-pip \
    wget

# Get chrome chromedriver for Selenium
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb

RUN pip3 install --no-cache --upgrade pip setuptools
WORKDIR /script/

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY ./get_demos.py .

CMD [ "python3", "./get_demos.py", "--offset=100"]