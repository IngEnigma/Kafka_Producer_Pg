FROM python:3.9-slim

# Instalar dependencias del sistema
RUN apt-get update && \
    apt-get install -y \
    wget \
    curl \
    default-jre \
    && rm -rf /var/lib/apt/lists/*

# Descargar e instalar Kafka
RUN wget https://dlcdn.apache.org/kafka/3.7.2/kafka_2.12-3.7.2.tgz && \
    tar -xvf kafka_2.12-3.7.2.tgz && \
    rm kafka_2.12-3.7.2.tgz

# Copiar archivos necesarios
COPY requirements.txt .
COPY run.sh .
COPY producer.py .  

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Dar permisos de ejecución
RUN chmod +x run.sh && \
    chmod +x kafka_2.12-3.7.2/bin/*.sh

# Exponer puertos
# Fast Api
EXPOSE 8000
# Kafka
EXPOSE 9092  

# Comando de inicio
CMD ["bash", "./run.sh"]
