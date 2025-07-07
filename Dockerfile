FROM bitnami/spark:latest

# Create app directory inside container
USER root
RUN mkdir -p /app/jars /app/scripts

# Copy local jars and scripts to container image
COPY jars/ /app/jars/
COPY scripts/ /app/scripts/

# Copy and install Python dependencies
COPY app/requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

ENV SPARK_APPLICATION_PYTHON_LOCATION=/app/scripts/kafka_stream.py
