FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:${PATH}"

WORKDIR /app

COPY pipeline/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY jars /app/jars

COPY pipeline /app/pipeline

COPY data /app/data

CMD ["python", "pipeline/pipeline.py"]