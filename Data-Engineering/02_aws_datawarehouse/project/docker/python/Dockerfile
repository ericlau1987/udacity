FROM python:3.8

# define default directory within docker
WORKDIR /data-engineering

# Copy requirements.txt
COPY ../../requirements-datawarehouse-project.txt requirements-datawarehouse-project.txt

# Install dependencies
RUN pip install -r requirements-datawarehouse-project.txt

CMD sleep infinity