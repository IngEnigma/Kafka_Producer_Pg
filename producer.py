import json
import requests
from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from typing import List
import logging

app = FastAPI()

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Ajusta según tu configuración
KAFKA_TOPIC = 'crimes'

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Crear productor Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def download_jsonl(url: str) -> List[dict]:
    """Descarga un archivo JSONL y lo convierte en una lista de diccionarios"""
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Parsear cada línea como JSON
        data = [json.loads(line) for line in response.text.splitlines()]
        return data
    except Exception as e:
        logger.error(f"Error al descargar o parsear JSONL: {str(e)}")
        raise

def send_to_kafka(data: List[dict]):
    """Envía los datos al tópico de Kafka"""
    try:
        for record in data:
            producer.send(KAFKA_TOPIC, value=record)
            logger.info(f"Enviado a Kafka: {record.get('dr_no', 'No DR_NO')}")
        
        producer.flush()
        return len(data)
    except Exception as e:
        logger.error(f"Error al enviar a Kafka: {str(e)}")
        raise

@app.post("/process-crimes")
async def process_crimes(url: str = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"):
    """
    Endpoint para procesar el archivo JSONL y enviar los datos a Kafka.
    
    Args:
        url: URL del archivo JSONL a procesar
        
    Returns:
        dict: Mensaje de éxito con cantidad de registros procesados
    """
    try:
        # Descargar datos
        logger.info(f"Descargando datos de {url}")
        crimes_data = download_jsonl(url)
        
        # Enviar a Kafka
        logger.info(f"Enviando {len(crimes_data)} registros a Kafka")
        records_sent = send_to_kafka(crimes_data)
        
        return {
            "message": "Datos procesados y enviados a Kafka exitosamente",
            "records_processed": records_sent,
            "kafka_topic": KAFKA_TOPIC
        }
    except Exception as e:
        logger.error(f"Error en el procesamiento: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
