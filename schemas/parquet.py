from pydantic import BaseModel

class ChartDataRequest(BaseModel):
    upload_id: str