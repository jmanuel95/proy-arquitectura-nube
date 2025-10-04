import json
import os
import logging
from typing import Dict, Any, List
import os
import io
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
ses = boto3.client("ses")
SES_FROM = os.environ.get("SES_FROM")


def build_txt_from_payload(payload: dict) -> bytes:
    """
    Genera un contenido .txt en base al payload.
    Campos esperados: EventName, EventDate, EventCountry, EventCity, name, email
    """
    lines = [
        "Comprobante de Registro de Evento",
        "---------------------------------",
        f"Evento:   {payload.get('EventName', '')}",
        f"Fecha:    {payload.get('EventDate', '')}",
        f"País:     {payload.get('EventCountry', '')}",
        f"Ciudad:   {payload.get('EventCity', '')}",
        f"Nombre:   {payload.get('name', '')}",
        f"Email:    {payload.get('email', '')}",
        "",
        "Documento generado automáticamente."
    ]
    text = "\n".join(lines)
    return text.encode("utf-8")

def send_email_with_txt_attachment(to_email: str, from_email: str, subject: str,
                                   body_text: str, attachment_name: str,
                                   attachment_bytes: bytes):
    """
    Envía correo via SES con adjunto .txt usando SendRawEmail.
    """
    if not from_email:
        raise ValueError("SES_FROM no está configurado en las variables de entorno")

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email

    # cuerpo en texto plano
    msg.attach(MIMEText(body_text, "plain", "utf-8"))

    # adjunto .txt
    part = MIMEApplication(attachment_bytes, _subtype="plain")
    part.add_header("Content-Disposition", "attachment", filename=attachment_name)
    msg.attach(part)

    # SES requiere raw email; enviamos como bytes
    ses.send_raw_email(RawMessage={"Data": msg.as_string().encode("utf-8")})

# --- dentro de tu handler, tras procesar exitosamente el mensaje SQS ---
# payload = {...}  # ya lo tienes parseado

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
    try:
        recipient = payload.get("email") or os.environ.get("SES_TO")  # fallback opcional
        if recipient:
            attachment = build_txt_from_payload(payload)
            subject = f"Comprobante de registro - {payload.get('EventName','')}".strip() or "Comprobante de registro"
            body = "Adjunto encontrarás tu comprobante de registro en formato .txt."
            send_email_with_txt_attachment(
                to_email=recipient,
                from_email=SES_FROM,
                subject=subject,
                body_text=body,
                attachment_name="comprobante.txt",
                attachment_bytes=attachment
            )
            logger.info("Correo enviado a %s con adjunto TXT", recipient)
        else:
            logger.warning("No hay email destino en payload ni SES_TO; no se envía correo.")
    except Exception as e:
        # No hacemos fail del batch por un error de correo; solo log
        logger.warning("No se pudo enviar correo SES: %s", e)
    return result