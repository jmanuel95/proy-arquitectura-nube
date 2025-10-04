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

    if http_method == "PUT":
        try:
            body = json.loads(event.get("body") or "{}")

            user_id = body.get("UserId")
            event_id = body.get("EventId")
            if not user_id or not event_id:
                return _resp(400, {"message": "UserId y EventId son obligatorios"})

            # Validar rol del usuario
            u = users_table.get_item(Key={"UserId": user_id})
            if "Item" not in u:
                return _resp(404, {"message": "Usuario no encontrado"})
            if u["Item"].get("role") != "ADMIN":
                return _resp(403, {"message": "Sólo ADMIN puede actualizar eventos"})

            # Campos permitidos para actualización (manda sólo los que quieras cambiar)
            allowed = ["EventName", "EventDate", "EventStatus", "EventCountry", "EventCity", "Quantity"]
            updates = {k: body[k] for k in allowed if k in body and body[k] is not None}

            if not updates:
                return _resp(400, {"message": "No hay campos válidos para actualizar"})

            # Validación de Quantity (si viene)
            if "Quantity" in updates:
                try:
                    q = int(updates["Quantity"])
                    if q < 0:
                        return _resp(400, {"message": "Quantity no puede ser negativo"})
                    updates["Quantity"] = q
                except Exception:
                    return _resp(400, {"message": "Quantity debe ser entero"})

            # Build UpdateExpression dinámico
            update_expr = "SET " + ", ".join(f"#{k} = :{k}" for k in updates.keys())
            expr_attr_names = {f"#{k}": k for k in updates.keys()}
            expr_attr_values = {f":{k}": updates[k] for k in updates.keys()}

            # Ejecutar actualización; falla si el evento no existe
            res = events_table.update_item(
                Key={"EventId": event_id},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_attr_names,
                ExpressionAttributeValues=expr_attr_values,
                ConditionExpression="attribute_exists(EventId)",
                ReturnValues="ALL_NEW"
            )

            return _resp(200, {"message": "Evento actualizado", "event": res.get("Attributes", {})})

        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return _resp(404, {"message": "Evento no encontrado"})
            return _resp(500, {"message": "Error actualizando evento", "error": str(e)})
        except Exception as e:
            return _resp(500, {"message": "Error inesperado", "error": str(e)})

    if http_method == "DELETE":
        try:
            body = json.loads(event.get("body") or "{}")
            event_id = body.get("EventId")

            if not event_id:
                return _resp(400, {"message": "EventId es obligatorio"})

            # Si quieres exigir ADMIN, descomenta este bloque:
            # user_id = body.get("UserId")
            # if not user_id:
            #     return _resp(400, {"message": "UserId es obligatorio para borrar"})
            # u = users_table.get_item(Key={"UserId": user_id})
            # if "Item" not in u or u["Item"].get("role") != "ADMIN":
            #     return _resp(403, {"message": "Sólo ADMIN puede eliminar eventos"})

            res = events_table.delete_item(
                Key={"EventId": event_id},
                ConditionExpression="attribute_exists(EventId)",  # 404 si no existe
                ReturnValues="ALL_OLD"
            )

            return _resp(200, {"message": "Evento eliminado", "event": res.get("Attributes", {})})

        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return _resp(404, {"message": "Evento no encontrado"})
            return _resp(500, {"message": "Error eliminando evento", "error": str(e)})
        except Exception as e:
            return _resp(500, {"message": "Error inesperado", "error": str(e)}) 