from pydantic import BaseModel, Field
from typing import List, Optional
from .chart import Chart

class Dashboard(BaseModel):
    mode: str
    upload_id: Optional[str] = None
    chart_id: str 