import boto3
import os
import json
import uuid
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# DynamoDB
ddb_res = boto3.resource("dynamodb")
events_table = ddb_res.Table(os.environ["EVENTS_TABLE"])
users_table = ddb_res.Table(os.environ["USERS_TABLE"])
registrations_table = ddb_res.Table(os.environ["REGISTRATION_TABLE"])
ddb_cli = boto3.client("dynamodb")

# SQS (NUEVO)
sqs = boto3.client("sqs")
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")  # Defínela en tu template/env

DISABLED_STATES = {"DESACTIVADO", "DESHABILITADO", "INHABILITADO"}

def _resp(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST,OPTIONS"
        },
        "body": json.dumps(body)
    }

# NUEVO: helper para enviar a SQS sin romper el flujo si falla
def send_to_sqs(payload: dict):
    if not SQS_QUEUE_URL:
        print("[INFO] SQS_QUEUE_URL no configurada; omitiendo envío a SQS")
        return
    params = {"QueueUrl": SQS_QUEUE_URL, "MessageBody": json.dumps(payload)}
    qname = SQS_QUEUE_URL.rsplit("/", 1)[-1]
    if qname.endswith(".fifo"):
        params["MessageGroupId"] = "registrations"
        params["MessageDeduplicationId"] = str(uuid.uuid4())
    try:
        sqs.send_message(**params)
    except ClientError as e:
        print(f"[WARN] Error enviando a SQS: {e}")

def handler(event, context):
    # CORS / método
    if event.get("httpMethod") == "OPTIONS":
        return _resp(200, {"ok": True})
    if event.get("httpMethod") and event["httpMethod"] != "POST":
        return _resp(405, {"message": "Method Not Allowed"})

    # Parseo del body
    try:
        body = event.get("body", event)
        if isinstance(body, str):
            body = json.loads(body or "{}")
    except Exception:
        return _resp(400, {"message": "Body must be valid JSON"})

    user_id = body.get("UserId")
    event_id = body.get("EventId")
    num = body.get("NumEntradas")

    # Validaciones básicas
    try:
        num = int(num)
    except Exception:
        return _resp(400, {"message": "NumEntradas debe ser un número entero >= 1"})
    if not user_id or not event_id or num < 1:
        return _resp(400, {"message": "UserId, EventId y NumEntradas>=1 son requeridos"})

    # 1) Verificar evento (AMPLIADO para traer más campos)
    try:
        evt = events_table.get_item(
            Key={"EventId": event_id},
            ProjectionExpression="#E, Quantity, EventStatus, EventName, EventDate, EventCountry, EventCity",
            ExpressionAttributeNames={"#E": "EventId"}
        ).get("Item")
    except ClientError as e:
        return _resp(500, {"message": "Error consultando evento", "detail": str(e)})

    if not evt:
        return _resp(404, {"message": "El evento no existe"})

    available = int(evt.get("Quantity", 0))
    status = str(evt.get("EventStatus", "")).upper()
    if status in DISABLED_STATES:
        return _resp(409, {"message": "El evento está desactivado"})
    if num > available:
        return _resp(409, {"message": "entradas no disponibles"})

    # 2) Verificar usuario
    try:
        u = users_table.get_item(Key={"UserId": user_id}).get("Item")
    except ClientError as e:
        return _resp(500, {"message": "Error consultando usuario", "detail": str(e)})

    if not u:
        return _resp(403, {"message": "Usuario no registrado. Debe registrarse antes de comprar."})

    # 3) Transacción: validar usuario, descontar stock y registrar compra
    reg_id = body.get("RegistrationId") or str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        ddb_cli.transact_write_items(
            TransactItems=[
                {
                    "ConditionCheck": {
                        "TableName": os.environ["USERS_TABLE"],
                        "Key": {"UserId": {"S": user_id}},
                        "ConditionExpression": "attribute_exists(UserId)"
                    }
                },
                {
                    "Update": {
                        "TableName": os.environ["EVENTS_TABLE"],
                        "Key": {"EventId": {"S": event_id}},
                        "UpdateExpression": "SET Quantity = Quantity - :q",
                        "ConditionExpression": (
                            "attribute_exists(EventId) AND Quantity >= :q AND "
                            "(attribute_not_exists(EventStatus) OR "
                            "(EventStatus <> :d1 AND EventStatus <> :d2 AND EventStatus <> :d3))"
                        ),
                        "ExpressionAttributeValues": {
                            ":q": {"N": str(num)},
                            ":d1": {"S": "DESACTIVADO"},
                            ":d2": {"S": "DESHABILITADO"},
                            ":d3": {"S": "INHABILITADO"}
                        }
                    }
                },
                {
                    "Put": {
                        "TableName": os.environ["REGISTRATION_TABLE"],
                        "Item": {
                            "RegistrationId": {"S": reg_id},
                            "RegistrationDate": {"S": now_iso},
                            "RegistrationEventId": {"S": event_id},
                            "RegistrationUserId": {"S": user_id},
                            "Quantity": {"N": str(num)}
                        },
                        "ConditionExpression": "attribute_not_exists(RegistrationId)"
                    }
                }
            ]
            # ReturnCancellationReasons no se usa para compatibilidad del runtime
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "TransactionCanceledException":
            # Concurrencia / sin stock / desactivado entre chequeo y transacción
            return _resp(409, {"message": "entradas no disponibles o evento desactivado"})
        return _resp(500, {"message": "Internal error", "detail": str(e)})
    except Exception as e:
        return _resp(500, {"message": "Internal error", "detail": str(e)})

    # 4) Si quedó en 0, marcar como DESHABILITADO
    try:
        events_table.update_item(
            Key={"EventId": event_id},
            UpdateExpression="SET EventStatus = :disabled",
            ConditionExpression="Quantity = :zero",
            ExpressionAttributeValues={":disabled": "DESHABILITADO", ":zero": 0}
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            # Enviamos igual a SQS aunque el estado no se haya podido actualizar
            pass

    # === NUEVO: construir payload y enviar a SQS ===
    # Fallbacks por si tu esquema de usuario usa otras claves (name/UserNames/UserName, email/UserEmail/Email)
    email = (u.get("email") or u.get("UserEmail") or u.get("Email") or "")
    name  = (u.get("name")  or u.get("UserNames") or u.get("UserName") or "")
    event_name    = str(evt.get("EventName", "") or "")
    event_date    = str(evt.get("EventDate", "") or "")
    event_country = str(evt.get("EventCountry", "") or "")
    event_city    = str(evt.get("EventCity", "") or "")

    sqs_payload = {
        "EventName": event_name,
        "EventDate": event_date,
        "EventCountry": event_country,
        "EventCity": event_city,
        "name": name,
        "email": email
    }
    send_to_sqs(sqs_payload)
    # === FIN NUEVO ===

    # OK
    return _resp(201, {
        "message": "Compra registrada",
        "RegistrationId": reg_id,
        "EventId": event_id,
        "UserId": user_id,
        "Quantity": num
    })
