import json
import requests
from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from typing import List
import logging
import atexit

app = FastAPI()

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Ajusta según tu configuración
KAFKA_TOPIC = 'crimes'
DATA_URL = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Variable global para almacenar los datos descargados
cached_data = None

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

def initialize_data():
    """Descarga los datos automáticamente al iniciar la aplicación"""
    global cached_data
    try:
        logger.info(f"Descargando datos iniciales de {DATA_URL}")
        cached_data = download_jsonl(DATA_URL)
        logger.info(f"Datos descargados exitosamente. Total de registros: {len(cached_data)}")
    except Exception as e:
        logger.error(f"Error al descargar datos iniciales: {str(e)}")
        # No levantamos la excepción para permitir que la API se inicie igual
        # pero cached_data permanecerá None

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

@app.on_event("startup")
async def startup_event():
    """Evento que se ejecuta al iniciar la aplicación"""
    initialize_data()

@app.post("/process-data")
async def process_data():
    """
    Endpoint para procesar los datos ya descargados y enviarlos a Kafka.
    
    Returns:
        dict: Mensaje de éxito con cantidad de registros procesados
    """
    global cached_data
    try:
        if cached_data is None:
            raise HTTPException(
                status_code=503,
                detail="Los datos no están disponibles. La descarga inicial falló o aún no se completó."
            )
        
        logger.info(f"Enviando {len(cached_data)} registros a Kafka")
        records_sent = send_to_kafka(cached_data)
        
        return {
            "message": "Datos procesados y enviados a Kafka exitosamente",
            "records_processed": records_sent,
            "kafka_topic": KAFKA_TOPIC
        }
    except Exception as e:
        logger.error(f"Error en el procesamiento: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@atexit.register
def shutdown():
    """Cierra el productor de Kafka al salir"""
    producer.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
