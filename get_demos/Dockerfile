# Using pypy to A) Test out speed B) only minimal requirments for
FROM pypy:3

WORKDIR /scipt/

COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "pypy3", "./get_demos.py", "--offset=100"]