import uuid
import pandas as pd
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from typing import Optional
from models.dataset import Dataset, AggregateRequest
from db.mongo import dataset_collection
from schemas.dataset import all_data

router = APIRouter()

EXPECTED_COLUMNS = ["model", "year", "region", "color", 
                    "transmission", "mileage_km", "price_usd", "sales_volume"]

@router.post("/upload")
async def upload_dataset(file: UploadFile = File(...)):
    """Handles CSV upload and validation"""
    try:
        df = pd.read_csv(file.file)
        df.columns = [col.strip().lower() for col in df.columns]
        col_map = {c.lower(): c for c in EXPECTED_COLUMNS}
        df = df[[col for col in df.columns if col in col_map]]
        df = df.rename(columns=col_map)

        for col in EXPECTED_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[EXPECTED_COLUMNS].where(pd.notnull(df), None)

        upload_id = f"upload_{uuid.uuid4().hex}"
        records = df.to_dict(orient="records")
        for idx, record in enumerate(records, start=1):
            record["upload_id"] = upload_id
            record["row_id"] = idx

        valid_records = []
        for rec in records:
            try:
                dataset = Dataset(**rec)
                valid_records.append(dataset.dict())
            except Exception as e:
                raise HTTPException(status_code=400, detail={"error": str(e), "row": rec})

        if valid_records:
            dataset_collection.insert_many(valid_records)

        return {"message": "CSV uploaded successfully", "upload_id": upload_id, "rows_inserted": len(valid_records)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/")
async def get_datasets(upload_id: Optional[str] = None):
    """Fetch datasets (optionally by upload_id)"""
    query = {"upload_id": upload_id} if upload_id else {}
    records = list(dataset_collection.find(query, {"_id": 0}))
    if not records:
        raise HTTPException(status_code=404, detail="No records found")
    return all_data(records)


@router.get("/headers")
async def get_headers(upload_id: Optional[str] = None):
    """Return headers that have at least one valid value"""
    query = {"upload_id": upload_id} if upload_id else {}
    records = list(dataset_collection.find(query, {"_id": 0}))
    if not records:
        raise HTTPException(status_code=404, detail="No records found")
    df = pd.DataFrame(records)
    return {"valid_headers": [col for col in df.columns if df[col].notnull().any()]}


@router.post("/aggregate")
async def aggregate(request: AggregateRequest):
    upload_id = request.upload_id
    x_axis = request.x_axis
    y_axis = request.y_axis
    agg_func = request.agg_func
    year_from = request.year_from
    year_to = request.year_to
    
    """Aggregate dataset directly in MongoDB"""
    funcs = {"sum": "$sum", "avg": "$avg", "count": "$sum", "min": "$min", "max": "$max"}
    if agg_func not in funcs:
        raise HTTPException(status_code=400, detail=f"Invalid agg_func. Choose from {list(funcs.keys())}")

    match_stage = {"upload_id": upload_id}
    if year_from or year_to:
        match_stage["year"] = {}
        if year_from:
            match_stage["year"]["$gte"] = int(year_from)
        if year_to:
            match_stage["year"]["$lte"] = int(year_to)

    pipeline = [
        {"$match": match_stage},
        {"$group": {
            "_id": f"${x_axis}",
            y_axis: ({"$sum": 1} if agg_func == "count" else {funcs[agg_func]: f"${y_axis}"})
        }},
        {"$project": {x_axis: "$_id", y_axis: f"${y_axis}", "_id": 0}},
        {"$sort": {x_axis: 1}}
    ]
    result = list(dataset_collection.aggregate(pipeline))
    if not result:
        raise HTTPException(status_code=404, detail="No records found")
    return result
