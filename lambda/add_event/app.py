import boto3
import os
import json
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from decimal import Decimal


dynamodb = boto3.resource("dynamodb")
events_table = dynamodb.Table(os.environ["EVENTS_TABLE"])
users_table = dynamodb.Table(os.environ["USERS_TABLE"])



def _resp(code, payload):
    def _default(o):
        if isinstance(o, Decimal):
            return int(o) if o % 1 == 0 else float(o)
        raise TypeError(f"Type not serializable: {type(o)}")
    return {
        "statusCode": code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload, ensure_ascii=False, default=_default)
    }



def handler(event, context):
    http_method = event.get('httpMethod', '')
    cors_headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    }
    if http_method == "POST":
        try:
            body = json.loads(event["body"])

            user_id = body.get("UserId")
            if not user_id:
                return {"statusCode": 400, "body": json.dumps({"error": "UserId es obligatorio"})}

            # Validar rol del usuario
            user_resp = users_table.get_item(Key={"UserId": user_id})
            if "Item" not in user_resp:
                return {"statusCode": 404, "body": json.dumps({"error": "Usuario no encontrado"})}

            role = user_resp["Item"].get("role")

            # Insrtar evento
            if role == "ADMIN":
                event_item = {
                    "EventId": body["EventId"],
                    "EventName": body["EventName"],
                    "EventDate": body["EventDate"],
                    "EventStatus": body["EventStatus"],
                    "EventCountry": body["EventCountry"],
                    "EventCity": body["EventCity"],
                    "UserId": body["UserId"],
                    "Quantity": body["Quantity"],
                    }
                events_table.put_item(Item=event_item)

                return {
                    "statusCode": 201,
                    "body": json.dumps({"message": "Evento creado exitosamente", "event": event_item})
                }

            else:
                return {"statusCode": 403, "body": json.dumps({"error": "Clientes no pueden crear eventos"})}

        except Exception as e:
            return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
    
    if http_method == "GET":
        try:
            items, last_key = [], None
            while True:
                params = {"FilterExpression": Attr("EventStatus").eq("Activo")}
                if last_key:
                    params["ExclusiveStartKey"] = last_key
                res = events_table.scan(**params)
                items.extend(res.get("Items", []))
                last_key = res.get("LastEvaluatedKey")
                if not last_key:
                    break

            return _resp(200, {"message": "Consulta exitosa", "count": len(items), "data": items})

        except ClientError as e:
            return _resp(500, {"message": "Error consultando eventos", "error": str(e)})
        except Exception as e:
            return _resp(500, {"message": "Error inesperado", "error": str(e)}) 