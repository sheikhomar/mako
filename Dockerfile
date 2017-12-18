FROM amazonlinux:latest

MAINTAINER Omar Ali Sheikh-Omar "sheikhomar@gmail.com"

RUN yum install python34 python34-devel python34-setuptools gcc -y \
    && curl https://bootstrap.pypa.io/get-pip.py --out get-pip.py \
    && python34 get-pip.py \
    && pip3 install python-dotenv>=0.5.1 \
    && pip3 install fastavro \
    && mkdir -p /app/src/data \
    && mkdir -p /app/data/raw

VOLUME ["/app/data/raw"]

ENV LC_ALL=en_GB.utf-8
ENV LANG=en_GB.utf-8

ADD src/data/prepare_dataset.py /app/src/data/prepare_dataset.py
ADD .env-sample /app/.env

ENTRYPOINT ["python34", "/app/src/data/prepare_dataset.py"]