import boto3
import os
import json

dynamodb = boto3.resource("dynamodb")
events_table = dynamodb.Table(os.environ["EVENTS_TABLE"])
users_table = dynamodb.Table(os.environ["USERS_TABLE"])

def handler(event, context):
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