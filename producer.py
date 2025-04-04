from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import requests
from kafka import KafkaProducer
import json
import logging
from typing import List, Dict

app = FastAPI()

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Ajusta según tu configuración
KAFKA_TOPIC = 'crimes'

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# URL del archivo JSONL
JSONL_URL = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"

# Configurar el producer de Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3
)

def download_and_parse_jsonl(url: str) -> List[Dict]:
    """Descarga y parsea un archivo JSONL desde una URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Parsear cada línea como un objeto JSON
        data = []
        for line in response.text.splitlines():
            if line.strip():  # Ignorar líneas vacías
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logger.error(f"Error al parsear línea: {line}. Error: {e}")
        
        return data
    except requests.RequestException as e:
        logger.error(f"Error al descargar el archivo: {e}")
        raise

@app.get("/trigger-download", response_class=JSONResponse)
async def trigger_download():
    """Endpoint para disparar la descarga y envío a Kafka."""
    try:
        # Descargar y parsear los datos
        crimes_data = download_and_parse_jsonl(JSONL_URL)
        
        if not crimes_data:
            raise HTTPException(status_code=404, detail="No se encontraron datos válidos en el archivo JSONL")
        
        # Enviar cada registro a Kafka
        for record in crimes_data:
            try:
                producer.send(KAFKA_TOPIC, value=record)
                logger.info(f"Enviado registro a Kafka: {record.get('dr_no', 'N/A')}")
            except Exception as e:
                logger.error(f"Error al enviar registro a Kafka: {e}")
        
        # Asegurar que todos los mensajes sean enviados
        producer.flush()
        
        return {
            "status": "success",
            "message": f"Datos enviados correctamente al tópico {KAFKA_TOPIC}",
            "records_sent": len(crimes_data)
        }
    except Exception as e:
        logger.error(f"Error en el proceso: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
