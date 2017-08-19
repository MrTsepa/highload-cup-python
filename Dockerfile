FROM ubuntu


RUN apt-get update \
  && apt-get install -y python3-pip python3-dev \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && pip3 install --upgrade pip

RUN pip install bottle pymongo bottle-mongo python-dateutil gevent

# Install MongoDB.
RUN \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6 \
    && echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.4.list \
    && apt-get update \
    && apt-get install -y mongodb-org

# Define mountable directories.
VOLUME ["/data/db"]

VOLUME ["/tmp/data"]

# Define working directory.
WORKDIR /data

# Expose ports
EXPOSE 80

ADD . /app
WORKDIR /app

ENV PYTHONENCODING=utf-8
ENV LANG=ru_RU.UTF-8

CMD mongod --fork --logpath mongodb_logs; \
    python unzip_data.py /tmp/data/data.zip; \
    python fill_db_full.py; \
    python run_gevent.py 0.0.0.0 80