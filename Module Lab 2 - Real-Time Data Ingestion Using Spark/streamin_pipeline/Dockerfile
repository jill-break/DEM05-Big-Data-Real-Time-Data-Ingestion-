FROM apache/spark:3.5.0-python3

USER root

# Install curl
RUN apt-get update && apt-get install -y curl

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

# Download Postgres Driver
RUN curl -o /opt/spark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

WORKDIR /app
USER spark