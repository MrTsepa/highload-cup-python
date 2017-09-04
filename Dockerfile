FROM ubuntu

RUN apt-get update \
  && apt-get install -y python python-dev python-pip \
  && pip install --upgrade pip


RUN pip install bottle pymongo bottle-mongo python-dateutil gevent

# Install MongoDB.
RUN \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6 \
    && echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.4.list \
    && apt-get update \
    && apt-get install -y mongodb-org



# Install Nginx.
RUN \
  apt-get install -y software-properties-common python-software-properties && \
  add-apt-repository -y ppa:nginx/stable && \
  apt-get update && \
  apt-get install -y nginx && \
  rm -rf /var/lib/apt/lists/* && \
  chown -R www-data:www-data /var/lib/nginx

# Define mountable directories.
VOLUME ["/etc/nginx/certs", "/etc/nginx/conf.d", "/var/log/nginx", "/var/www/html", "/var/cache/nginx"]


# Define mountable directories.
VOLUME ["/data/db"]

VOLUME ["/tmp/data"]

# Define working directory.
WORKDIR /data

# Expose ports
EXPOSE 80

COPY nginx.conf /etc/nginx/nginx.conf
COPY nginx_balance.conf /etc/nginx/sites-enabled/default

ADD . /app
WORKDIR /app

CMD python prepare_nginx.py /tmp/data; \
    nginx; \
    mongod --fork --logpath mongodb_logs; \
    python fill_db_full.py /tmp/data/data.zip; \
    tail -10 mongodb_logs; \
    python run_multi_gevent.py 0.0.0.0 8080 8081 8082 8083

#    python run_gevent.py 0.0.0.0 80