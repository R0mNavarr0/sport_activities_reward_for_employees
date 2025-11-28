FROM eclipse-temurin:17-jdk

# Versions
ENV SPARK_VERSION=4.0.1
ENV HADOOP_VERSION=3
ENV SCALA_BINARY_VERSION=2.13

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN apt-get update && \
    apt-get install -y curl bash && \
    rm -rf /var/lib/apt/lists/*

# Spark 4.0.1 précompilé (Scala 2.13 inside), pas de suffixe -scala2.13 dans le nom du .tgz
RUN curl -fL "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
      -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark.tgz

# JAR Delta 4.0.1 (Scala 2.13)
RUN mkdir -p $SPARK_HOME/jars && \
    curl -fL "https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_BINARY_VERSION}/4.0.0/delta-spark_${SCALA_BINARY_VERSION}-4.0.0.jar" \
    -o "$SPARK_HOME/jars/delta-spark_${SCALA_BINARY_VERSION}-4.0.0.jar" && \
    curl -fL "https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar" \
    -o "$SPARK_HOME/jars/delta-storage-4.0.0.jar"

# Script d'entrée Thrift
COPY ./spark/spark-thrift-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /opt/spark
ENTRYPOINT ["/entrypoint.sh"]