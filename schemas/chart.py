from pydantic import BaseModel
from typing import Optional

class AggregateRequest(BaseModel):
    upload_id: Optional[str] = None
    x_axis: str
    y_axis: str
    agg_func: str = "sum"
    year_from: Optional[int] = None
    year_to: Optional[int] = None


class Chart(BaseModel):
    mode: str
    upload_id: Optional[str] = None
    chart_type: str
    x_axis: str
    y_axis: str
    agg_func: str
    year_from: Optional[int] = None
    year_to: Optional[int] = None
    name: Optional[str] = None
    chart_library: str
