from pydantic import BaseModel
from typing import Optional

class Dataset(BaseModel):
    upload_id: str
    row_id: int
    model: Optional[str] = None
    year: Optional[int] = None
    region: Optional[str] = None
    color: Optional[str] = None
    transmission: Optional[str] = None
    mileage_km: Optional[float] = None
    price_usd: Optional[float] = None
    sales_volume: Optional[int] = None
