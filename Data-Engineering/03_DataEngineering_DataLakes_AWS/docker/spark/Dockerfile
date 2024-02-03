FROM bitnami/spark:latest

# define default directory within docker
# WORKDIR /data-engineering

# Copy requirements.txt
COPY ../../requirements.txt requirements.txt

# Install dependencies
RUN pip install -r requirements.txt

# CMD sleep infinity