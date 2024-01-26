FROM python:3.8

# define default directory within docker
WORKDIR /data-engineering

# Copy requirements.txt
COPY ../../requirements.txt requirements.txt

# Install dependencies
# RUN pip install --upgrad pip
RUN pip install -r requirements.txt

# COPY ../../.env .env
# CMD dbt deps && dbt build && sleep infinity
CMD sleep infinity
# CMD dbt debug -t dev && dbt debug -t prod && sleep infinity