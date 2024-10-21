# Base image
FROM openjdk:8-jdk-slim

# Install Python and dependencies
RUN apt-get update && \
    apt-get install -y python3-pip python3 wget procps && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean

# Install Spark
RUN wget -O - https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz| tar -xz -C /opt/
ENV SPARK_HOME=/opt/spark-3.5.3-bin-hadoop3
ENV PATH=$PATH:$SPARK_HOME/bin

# Copy project files
WORKDIR /app
COPY . /app

# Install Python dependencies
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Define entrypoint to use Python
ENTRYPOINT ["python3"]

# Default command for running the main script
CMD ["main.py"]
