import pandas as pd
import numpy as np
from fastapi import APIRouter, UploadFile, File, HTTPException
from pymongo import ASCENDING
from lib.utils import generate_short_uuid
from models.schema_less import schema_less_collection
from schemas.schema_less import SchemalessAggregateRequest

router = APIRouter(prefix="/schemaless", tags=["Schema Less"])

# Upload
@router.post("/upload")
async def upload_dataset(file: UploadFile = File(...)):
    try:
        df = pd.read_csv(file.file)
        df.columns = [col.strip().lower() for col in df.columns]

        df = df.apply(pd.to_numeric, errors='ignore')

        df = df.replace([np.inf, -np.inf], np.nan)

        df = df.replace({pd.NA: None, np.nan: None})

        upload_id = f"{generate_short_uuid()}"
        records = df.to_dict(orient="records")

        for idx, record in enumerate(records, start=1):
            record["upload_id"] = upload_id
            record["row_id"] = idx

        def is_convertible_to_number(value:str):
            try:
                float(value)
                return True
            except ValueError:
                return False
            

        for record in records:
            for key, value in record.items():
                if value and is_convertible_to_number(value):
                    record[key] = float(value)
                elif value == None:
                    record[key] = ""

        schema_less_collection.insert_many(records)

        return {
            "message": "CSV uploaded successfully",
            "upload_id": upload_id,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/{upload_id}/data")
async def get_dataset_contents(upload_id: str):
    query = {"upload_id": upload_id}
    records = list(schema_less_collection.find(query, {"_id": 0}))
    if not records:
        raise HTTPException(status_code=404, detail="No records found for this upload_id")
    return records

@router.get("/{upload_id}/headers")
async def get_headers(upload_id: str):
    query = {"upload_id": upload_id}
    records = list(schema_less_collection.find(query, {"_id": 0}))
    if not records:
        raise HTTPException(status_code=404, detail="No records found")

    valid_headers = set()
    for record in records:
        for key, value in record.items():
            if value not in (None, "", [], {}):
                valid_headers.add(key)

    return {"valid_headers": sorted(valid_headers)}

# Get all unique upload_ids
@router.get("/all")
async def get_all_upload_ids():
    """Returns all unique upload_id values"""
    upload_ids = schema_less_collection.distinct("upload_id")
    return {"upload_ids": upload_ids or []}


@router.post("/aggregate")
async def schemaless_aggregate(request: SchemalessAggregateRequest):
    """
    Aggregates schemaless dataset fields dynamically based on user-selected x/y axes.
    """
    upload_id = request.upload_id
    x_axis = request.x_axis
    y_axis = request.y_axis
    agg_func = request.agg_func
    buckets = request.buckets


    funcs = {"sum": "$sum", "avg": "$avg", "count": "$sum", "min": "$min", "max": "$max"}
    if agg_func not in funcs:
        raise HTTPException(status_code=400, detail=f"Invalid agg_func. Choose from {list(funcs.keys())}")

    match_stage = {"upload_id": upload_id}

    # --- Detect continuous vs categorical fields ---
    sample_docs = list(schema_less_collection.find(match_stage, {x_axis: 1, y_axis: 1}).limit(100))

    def is_continuous(field: str) -> bool:
        values = [d.get(field) for d in sample_docs if isinstance(d.get(field), (int, float))]
        if not values:
            return False
        return len(set(values)) > 15  # heuristic threshold

    x_is_continuous = is_continuous(x_axis)
    y_is_continuous = is_continuous(y_axis)

    # --- Build aggregation pipeline ---
    if x_is_continuous and y_is_continuous:
        # Trend-type bucket aggregation
        pipeline = [
            {"$match": match_stage},
            {
                "$bucketAuto": {
                    "groupBy": f"${x_axis}",
                    "buckets": buckets,
                    "output": {
                        f"avg_{y_axis}": {"$avg": f"${y_axis}"},
                        "count": {"$sum": 1},
                    },
                }
            },
            {"$project": {
                "x_range_min": "$_id.min",
                "x_range_max": "$_id.max",
                f"avg_{y_axis}": 1,
                "count": 1,
                "_id": 0,
            }},
            {"$sort": {"x_range_min": ASCENDING}},
        ]

    elif x_is_continuous:
        # Continuous X, categorical Y
        pipeline = [
            {"$match": match_stage},
            {
                "$bucketAuto": {
                    "groupBy": f"${x_axis}",
                    "buckets": buckets,
                    "output": {
                        f"{y_axis}": (
                            {"$sum": 1}
                            if agg_func == "count"
                            else {funcs[agg_func]: f"${y_axis}"}
                        ),
                        "count": {"$sum": 1},
                    },
                }
            },
            {"$project": {
                "x_range_min": "$_id.min",
                "x_range_max": "$_id.max",
                f"{y_axis}": 1,
                "count": 1,
                "_id": 0,
            }},
            {"$sort": {"x_range_min": ASCENDING}},
        ]

    else:
        # Both categorical — simple group
        pipeline = [
            {"$match": match_stage},
            {"$group": {
                "_id": f"${x_axis}",
                y_axis: (
                    {"$sum": 1}
                    if agg_func == "count"
                    else {funcs[agg_func]: f"${y_axis}"}
                ),
            }},
            {"$project": {x_axis: "$_id", y_axis: f"${y_axis}", "_id": 0}},
            {"$sort": {x_axis: ASCENDING}},
        ]

    # --- Execute and return ---
    try:
        result = list(schema_less_collection.aggregate(pipeline))
        if not result:
            raise HTTPException(status_code=404, detail="No matching data found for aggregation")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))