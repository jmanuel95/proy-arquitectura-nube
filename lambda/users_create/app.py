import json, os, datetime, boto3
from botocore.exceptions import ClientError 

TABLE_NAME = os.environ.get("TABLE_NAME", "")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
ALLOWED_ROLES = {"CLIENTE", "ADMIN"}

def _resp(code, payload):
    return {"statusCode": code, "headers": {"Content-Type":"application/json"}, "body": json.dumps(payload)}

def handler(event, context):
    try:
        body = event.get("body") or ""
        if event.get("isBase64Encoded"):
            import base64
            body = base64.b64decode(body).decode("utf-8")
        data = json.loads(body or "{}")

        user_id = (data.get("userId") or "").strip()
        email   = (data.get("email") or "").strip()
        name    = (data.get("name") or "").strip()
        role    = (data.get("role") or "").strip().upper()

        if not (user_id and email and name and role):
            return _resp(400, {"error":"Faltan campos: userId, email, name, role"})
        if role not in ALLOWED_ROLES:
            return _resp(400, {"error": f"role inválido. Permitidos: {sorted(ALLOWED_ROLES)}"})

        now = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        item = {
            "UserId": user_id,      # ⬅️ CLAVE PRIMARIA REAL DE TU TABLA
            "email": email,
            "name": name,
            "role": role,
            "createdAt": now
        }

        table.put_item(Item=item, ConditionExpression="attribute_not_exists(UserId)")
        return _resp(201, {"message":"Usuario creado", "userId": user_id})

    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ConditionalCheckFailedException":
            return _resp(409, {"error":"El usuario ya existe"})
        return _resp(500, {"error":"Error DynamoDB", "code": code, "message": e.response["Error"].get("Message")})
    except Exception as e:
        print("ERROR:", repr(e))
        return _resp(500, {"error":"Internal error"})