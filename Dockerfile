FROM gliderlabs/alpine:latest

RUN apk add --update \
    python \
    python-dev \
    py-pip \
    build-base \
  && pip install virtualenv \
  && rm -rf /var/cache/apk/*

ADD forecast.py forecast.py
RUN chmod +x forecast.py

WORKDIR /app

ONBUILD COPY . /app
ONBUILD RUN virtualenv /env && /env/bin/pip install -r /app/requirements.txt

CMD ["python" "/app/forecast.py"]

