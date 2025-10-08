import uuid

# shorter UUID format: xxxx-xxxx-xxxx
def generate_short_uuid():
    u = uuid.uuid4().hex  # 32 hex chars
    return f"{u[:4]}-{u[4:8]}-{u[8:12]}"
