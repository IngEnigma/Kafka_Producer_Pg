#!/bin/bash

# Iniciar Zookeeper en segundo plano
kafka_2.12-3.7.2/bin/zookeeper-server-start.sh kafka_2.12-3.7.2/config/zookeeper.properties &

sleep 5

# Iniciar Kafka en segundo plano
kafka_2.12-3.7.2/bin/kafka-server-start.sh kafka_2.12-3.7.2/config/server.properties &

sleep 10

# Crear el topic 'crimes' si no existe
kafka_2.12-3.7.2/bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic crimes || \
    echo "El topic crimes ya existe o no se pudo crear"

sleep 5

# Iniciar la API FastAPI
uvicorn api_crimes_kafka:app --host 0.0.0.0 --port 8000 &

# Mantener el contenedor en ejecución
tail -f /dev/null
