from pydantic import BaseModel

class SchemalessAggregateRequest(BaseModel):
    upload_id: str
    x_axis: str
    y_axis: str
    agg_func: str = "sum"
    buckets: int = 20
