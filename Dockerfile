FROM apache/airflow:2.8.1

USER root

# Install Java and Spark
RUN apt-get update && apt-get install -y openjdk-11-jdk wget && \
    wget https://downloads.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.1-bin-hadoop3.tgz && \
    mv spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm spark-3.5.1-bin-hadoop3.tgz && \
    ln -s /opt/spark/bin/spark-submit /usr/local/bin/spark-submit && \
    ln -s /opt/spark/bin/spark-shell /usr/local/bin/spark-shell

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$PATH:/opt/spark/bin"

USER airflow