FROM mysql:7.0.1

COPY .env .env

COPY docker/db_init.sh /docker-entrypoint-initdb.d/db_init.sh

EXPOSE 8088
