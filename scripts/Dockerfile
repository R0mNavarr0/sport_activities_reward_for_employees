FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:${PATH}"

WORKDIR /app

COPY scripts/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY jars /app/jars

COPY scripts /app/scripts

COPY data /app/data

CMD ["python", "scripts/ETL_Full_Load/pipeline.py"]