FROM influxdb:2.7

# Copy init scripts and sample data that run on first startup
COPY docker-entrypoint-initdb.d /docker-entrypoint-initdb.d

RUN chmod +x /docker-entrypoint-initdb.d/*.sh