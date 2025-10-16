import uuid
import pandas as pd
from fastapi import APIRouter, UploadFile, File, HTTPException
from lib.utils import generate_short_uuid
from schemas.dataset import Dataset
from models.dataset import dataset_collection
from serializers.dataset import all_data

router = APIRouter(prefix="/dataset", tags=["Dataset"])

EXPECTED_COLUMNS = [
    "model",
    "year",
    "region",
    "color",
    "transmission",
    "mileage_km",
    "price_usd",
    "sales_volume",
]

# Upload CSV
@router.post("/upload")
async def upload_dataset(file: UploadFile = File(...)):
    """Handles CSV upload and validation"""
    try:
        df = pd.read_csv(file.file)
        df.columns = [col.strip().lower() for col in df.columns]
        col_map = {c.lower(): c for c in EXPECTED_COLUMNS}
        df = df[[col for col in df.columns if col in col_map]]
        df = df.rename(columns=col_map)

        # Add missing columns as None
        for col in EXPECTED_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[EXPECTED_COLUMNS].where(pd.notnull(df), None)

        upload_id = f"{generate_short_uuid()}"
        records = df.to_dict(orient="records")

        for idx, record in enumerate(records, start=1):
            record["upload_id"] = upload_id
            record["row_id"] = idx

        valid_records = [Dataset(**rec).dict() for rec in records]

        if valid_records:
            dataset_collection.insert_many(valid_records)

        return {
            "message": "CSV uploaded successfully",
            "upload_id": upload_id,
            "rows_inserted": len(valid_records),
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Get all unique upload_ids
@router.get("/all")
async def get_all_upload_ids():
    """Returns all unique upload_id values"""
    upload_ids = dataset_collection.distinct("upload_id")
    return {"upload_ids": upload_ids or []}


# Get all data across all uploads
@router.get("/all/data")
async def get_all_data():
    """Returns all records across all uploads"""
    records = list(dataset_collection.find({}, {"_id": 0}))
    if not records:
        raise HTTPException(status_code=404, detail="No records found")
    return all_data(records)


# Get all unique headers
@router.get("/all/headers")
async def get_all_headers():
    """Returns all unique headers across all datasets (no pandas)"""
    records = list(dataset_collection.find({}, {"_id": 0}))
    if not records:
        raise HTTPException(status_code=404, detail="No records found")

    # Collect all keys that have at least one non-null value
    valid_headers = set()
    for record in records:
        for key, value in record.items():
            if value not in (None, "", [], {}):
                valid_headers.add(key)

    return {"valid_headers": sorted(valid_headers)}



# Get all dataset contents
@router.get("/{upload_id}/data")
async def get_dataset_contents(upload_id: str):
    """Returns all records for a specific upload_id"""
    query = {"upload_id": upload_id}
    records = list(dataset_collection.find(query, {"_id": 0}))
    if not records:
        raise HTTPException(status_code=404, detail="No records found for this upload_id")
    return all_data(records)


# Get all headers per upload
@router.get("/{upload_id}/headers")
async def get_headers(upload_id: str):
    """Returns all headers for a given upload_id (no pandas)"""
    query = {"upload_id": upload_id}
    records = list(dataset_collection.find(query, {"_id": 0}))
    if not records:
        raise HTTPException(status_code=404, detail="No records found")

    valid_headers = set()
    for record in records:
        for key, value in record.items():
            if value not in (None, "", [], {}):
                valid_headers.add(key)

    return {"valid_headers": sorted(valid_headers)}
