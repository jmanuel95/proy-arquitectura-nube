import boto3
import os
import json
import uuid
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# Mismo estilo: resource para tablas + client para TransactWrite
dynamo_resource = boto3.resource("dynamodb")
events_table = dynamo_resource.Table(os.environ["EVENTS_TABLE"])
users_table = dynamo_resource.Table(os.environ["USERS_TABLE"])
registrations_table = dynamo_resource.Table(os.environ["REGISTRATION_TABLE"])

dynamo_client = boto3.client("dynamodb")

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
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return _resp(200, {"ok": True})
    if event.get("httpMethod") and event["httpMethod"] != "POST":
        return _resp(405, {"message": "Method Not Allowed"})

    # Body JSON
    try:
        body = event.get("body", event)
        if isinstance(body, str):
            body = json.loads(body or "{}")
    except Exception:
        return _resp(400, {"message": "Body must be valid JSON"})

    user_id = body.get("UserId")
    event_id = body.get("EventId")
    qty = int(body.get("Quantity", 1))

    if not user_id or not event_id or qty < 1:
        return _resp(400, {"message": "UserId, EventId y Quantity>=1 son requeridos"})

    # Validar que el usuario exista (respuesta 404 si no)
    try:
        user_resp = users_table.get_item(Key={"UserId": user_id})
        if "Item" not in user_resp:
            return _resp(404, {"message": "Usuario no encontrado"})
    except ClientError as e:
        return _resp(500, {"message": "Error consultando usuario", "detail": str(e)})

    # Datos registro
    reg_id = body.get("RegistrationId") or str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Transacción: 1) usuario existe, 2) descontar stock si hay suficiente, 3) insertar registro
    try:
        dynamo_client.transact_write_items(
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
                        "ConditionExpression": "attribute_exists(EventId) AND Quantity >= :q",
                        "ExpressionAttributeValues": {":q": {"N": str(qty)}}
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
                            "Quantity": {"N": str(qty)}
                        },
                        "ConditionExpression": "attribute_not_exists(RegistrationId)"
                    }
                }
            ],
            ReturnCancellationReasons=True  # para diferenciar causas de error
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "TransactionCanceledException":
            # Inspeccionar razones para mensajes más claros
            reasons = e.response.get("CancellationReasons") or []
            # Si el update del evento falló por condición → sin stock suficiente
            for r in reasons:
                if (r.get("Code") == "ConditionalCheckFailed" and
                    r.get("Message") and "Quantity" in r.get("Message", "")):
                    return _resp(409, {"message": "cantidad de entradas no disponibles"})
            # Genérico: usuario no existe, evento no existe o id duplicado
            return _resp(409, {"message": "Transacción cancelada: verifique usuario, evento y stock"})
        return _resp(500, {"message": "Internal error", "detail": str(e)})
    except Exception as e:
        return _resp(500, {"message": "Internal error", "detail": str(e)})

    # Si la transacción fue OK, actualizar estado a INHABILITADO si quedó en 0
    try:
        events_table.update_item(
            Key={"EventId": event_id},
            UpdateExpression="SET EventStatus = :disabled",
            ConditionExpression="Quantity = :zero",
            ExpressionAttributeValues={
                ":disabled": "INHABILITADO",
                ":zero": 0
            }
        )
    except ClientError as e:
        # Si falla por condición, simplemente no estaba en cero; ignorar
        if e.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            # Si es otro error de AWS, devuélvelo como warning (no romper la compra)
            return _resp(201, {
                "message": "Compra registrada (warning: no se pudo actualizar estado)",
                "RegistrationId": reg_id,
                "EventId": event_id,
                "UserId": user_id,
                "Quantity": qty,
                "warn": str(e)
            })

    # Éxito
    return _resp(201, {
        "message": "Compra registrada",
        "RegistrationId": reg_id,
        "EventId": event_id,
        "UserId": user_id,
        "Quantity": qty
    })
