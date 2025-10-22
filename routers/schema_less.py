import pandas as pd
import numpy as np
from fastapi import APIRouter, UploadFile, File, HTTPException
from pymongo import ASCENDING
from lib.utils import detect_column_type, generate_short_uuid
from models.schema_less import schema_less_collection
from schemas.schema_less import SchemalessAggregateRequest
from models.dataset_metadata import dataset_metadata_collection

router = APIRouter(prefix="/schemaless", tags=["Schema Less"])

# Upload
@router.post("/upload")
async def upload_dataset(file: UploadFile = File(...)):
    try:
        df = pd.read_csv(file.file)
        df.columns = [col.strip().lower() for col in df.columns]

        # df = df.apply(pd.to_numeric, errors='ignore')

        # df = df.replace([np.inf, -np.inf], np.nan)

        # df = df.replace({pd.NA: None, np.nan: None})

        upload_id = f"{generate_short_uuid()}"
        records = df.to_dict(orient="records")

        for idx, record in enumerate(records, start=1):
            record["upload_id"] = upload_id
            record["row_id"] = idx

        # def is_convertible_to_number(value:str):
        #     try:
        #         float(value)
        #         return True
        #     except ValueError:
        #         return False
            

        # for record in records:
        #     for key, value in record.items():
        #         if value and is_convertible_to_number(value):
        #             record[key] = float(value)
        #         elif value == None:
        #             record[key] = ""

        schema_less_collection.insert_many(records)

        # Detect column types
        column_types = {col: detect_column_type(df[col]) for col in df.columns}

        # Store metadata in a separate collection
        dataset_metadata_collection.insert_one({
            "upload_id": upload_id,
            "column_types": column_types,
            "created_at": pd.Timestamp.now().isoformat()
        })

        return {
            "message": "CSV uploaded successfully",
            "upload_id": upload_id,
            "column_types": column_types,
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    

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

    ignored_columns = {"upload_id", "row_id"}

    valid_headers = set()
    for record in records:
        for key, value in record.items():
            if key not in ignored_columns and value not in (None, "", [], {}):
                valid_headers.add(key)

    # Try to get stored column types
    metadata = dataset_metadata_collection.find_one({"upload_id": upload_id}, {"_id": 0, "column_types": 1})
    column_types = metadata["column_types"] if metadata else {}

    return {
        "valid_headers": sorted(valid_headers),
        "column_types": column_types,
    }

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


    funcs = {"sum": "$sum", "avg": "$avg", "count": "$sum", "min": "$min", "max": "$max"}
    if agg_func not in funcs:
        raise HTTPException(status_code=400, detail=f"Invalid agg_func. Choose from {list(funcs.keys())}")

    match_stage = {"upload_id": upload_id}


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