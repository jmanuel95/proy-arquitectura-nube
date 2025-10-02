import json
import os
import logging
from typing import Dict, Any, List

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

def _parse_json(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s)
    except Exception:
        return {}

def handler(event, context):
    """
    SQS -> Lambda (batch). Usa 'ReportBatchItemFailures' para fallas parciales.
    Espera body tipo:
      {
        "EventName": "...",
        "EventDate": "YYYY-MM-DD",
        "EventCountry": "...",
        "EventCity": "...",
        "name": "...",
        "email": "..."
      }
    """
    logger.info("Received %d records", len(event.get("Records", [])))
    failures: List[Dict[str, str]] = []

    for record in event.get("Records", []):
        msg_id = record.get("messageId")
        body_raw = record.get("body", "")
        payload = _parse_json(body_raw)

        if not payload:
            logger.warning("JSON inválido en messageId=%s body=%s", msg_id, body_raw)
            failures.append({"itemIdentifier": msg_id})
            continue

        # Extrae campos (si falta alguno, registra warning pero no falla el batch)
        event_name    = payload.get("EventName")
        event_date    = payload.get("EventDate")
        event_country = payload.get("EventCountry")
        event_city    = payload.get("EventCity")
        name          = payload.get("name")
        email         = payload.get("email")

        logger.info(
            "Procesando registro: event_name=%s, date=%s, country=%s, city=%s, name=%s, email=%s",
            event_name, event_date, event_country, event_city, name, email
        )

        # TODO: Aquí tu lógica real (enviar email, notificar, guardar auditoría, etc.)
        # Si algo falla de verdad para este mensaje en particular:
        # failures.append({"itemIdentifier": msg_id}); continue

    # Respuesta para SQS partial batch
    result = {"batchItemFailures": failures}
    logger.info("Batch result: %s", result)
    return result