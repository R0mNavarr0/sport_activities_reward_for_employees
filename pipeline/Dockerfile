FROM python:3.12-slim

# --- Installer Java (JDK via meta-package Debian) ---
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

# Sur Debian, default-jdk pointe vers la version par défaut (17 ou plus)
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:${PATH}"

WORKDIR /app

# 1) Copier et installer les dépendances
COPY pipeline/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY jars /app/jars

# 2) Copier tout le dossier pipeline (y compris pipeline.py et les scripts ETL)
COPY pipeline /app/pipeline

# 3) Copier les données d'entrée
COPY data /app/data

# 3) Lancer la pipeline
CMD ["python", "pipeline/pipeline.py"]