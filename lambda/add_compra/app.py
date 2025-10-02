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

    # 1) Verificar evento
    try:
        evt = events_table.get_item(
            Key={"EventId": event_id},
            ProjectionExpression="#E, Quantity, EventStatus",
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
            ],
            #ReturnCancellationReasons=True
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
        # Si la condición falla, simplemente no quedó en 0. Otros errores → warning en respuesta.
        if e.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            return _resp(201, {
                "message": "Compra registrada (warning: no se pudo actualizar estado)",
                "RegistrationId": reg_id,
                "EventId": event_id,
                "UserId": user_id,
                "Quantity": num,
                "warn": str(e)
            })

    # OK
    return _resp(201, {
        "message": "Compra registrada",
        "RegistrationId": reg_id,
        "EventId": event_id,
        "UserId": user_id,
        "Quantity": num
    })
