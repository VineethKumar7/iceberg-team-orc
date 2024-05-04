# Use an official OpenJDK runtime as a base image
FROM openjdk:11-jdk-slim

# Set environment variables for Spark and Hadoop versions
ENV SPARK_VERSION=3.4.3
ENV HADOOP_VERSION=3
ENV HADOOP_AWS_VERSION=3.2.2
ENV ICEBERG_VERSION=1.5.1
ENV AWS_SDK_VERSION=1.11.901
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Install necessary packages
RUN apt-get update && apt-get install -y \
    curl \
    bash \
    python3 \
    python3-pip \
    procps \
    nano && \
    rm -rf /var/lib/apt/lists/*

# Download and un-tar Apache Spark
RUN curl -sL --retry 3 \
    "https://downloads.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" \
    | tar -xz -C /opt/ && \
    mv /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME

# Install Python dependencies for PySpark
RUN pip3 install --no-cache-dir pyspark==$SPARK_VERSION findspark

# Add Iceberg libraries if needed, adjust version accordingly

RUN curl -sL --retry 3 \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/$ICEBERG_VERSION/iceberg-spark-runtime-3.5_2.12-$ICEBERG_VERSION.jar" \
    -o $SPARK_HOME/jars/iceberg-spark-runtime-3.5_2.12-$ICEBERG_VERSION.jar

# Download and add necessary JARs to the Spark jars folder
RUN curl -sL --retry 3 \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_AWS_VERSION/hadoop-aws-$HADOOP_AWS_VERSION.jar" \
    -o $SPARK_HOME/jars/hadoop-aws-$HADOOP_AWS_VERSION.jar && \
    curl -sL --retry 3 \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/$AWS_SDK_VERSION/aws-java-sdk-bundle-$AWS_SDK_VERSION.jar" \
    -o $SPARK_HOME/jars/aws-java-sdk-bundle-$AWS_SDK_VERSION.jar
	

# Create a directory for scripts and other related files
RUN mkdir -p /home/spark/scripts


# Copy necessary config files and scripts to Docker image
COPY start-spark.sh /opt/
COPY lineitem.csv /home/spark/scripts/
COPY iceberg_orc.py /home/spark/scripts/
RUN chmod +x /opt/start-spark.sh

# Expose ports for Spark UI and other services
EXPOSE 8080 7077 8888

# Start Spark and include Iceberg
CMD ["/bin/bash", "/opt/start-spark.sh"]
