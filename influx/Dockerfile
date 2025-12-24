FROM influxdb:3.8-core

# Copy init scripts and sample data that run on first startup
COPY docker-entrypoint-initdb.d /docker-entrypoint-initdb.d

# Make init scripts executable
RUN find /docker-entrypoint-initdb.d -name "*.sh" -type f -exec chmod +x {} \; || true