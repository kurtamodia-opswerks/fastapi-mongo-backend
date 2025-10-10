from pydantic import BaseModel
from typing import Optional

class Dashboard(BaseModel):
    mode: str
    upload_id: Optional[str] = None
    chart_id: Optional[str] = None  # made optional since not needed on update
    year_from: Optional[int] = None
    year_to: Optional[int] = None

class DashboardUpdate(BaseModel):
    year_from: Optional[int] = None
    year_to: Optional[int] = None

