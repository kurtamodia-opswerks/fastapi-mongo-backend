from pydantic import BaseModel
from typing import Optional

class User(BaseModel):
    email: str
    name: str
    image: Optional[str] = None

