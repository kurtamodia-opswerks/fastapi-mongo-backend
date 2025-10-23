from collections import Counter, defaultdict
import uuid
import pandas as pd
from fastapi import APIRouter, UploadFile, File, HTTPException
from lib.utils import detect_column_type, generate_short_uuid
from schemas.dataset import Dataset
from models.dataset import dataset_collection
from models.dataset_metadata import dataset_metadata_collection
from serializers.dataset import all_data
from lib.ws_manager import manager

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
    """Handles CSV upload and saves dataset + column type metadata"""
    try:
        df = pd.read_csv(file.file)
        df.columns = [col.strip().lower() for col in df.columns]
        col_map = {c.lower(): c for c in EXPECTED_COLUMNS}
        df = df[[col for col in df.columns if col in col_map]]
        df = df.rename(columns=col_map)

        num_duplicates = int(df.duplicated().sum())

        # Drop duplicate rows (keep the first occurrence)
        df = df.drop_duplicates(subset=EXPECTED_COLUMNS, keep="first")

        # Add missing columns as None
        for col in EXPECTED_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[EXPECTED_COLUMNS].where(pd.notnull(df), None)


        upload_id = generate_short_uuid()
        records = df.to_dict(orient="records")

        for idx, record in enumerate(records, start=1):
            record["upload_id"] = upload_id
            record["row_id"] = idx

        valid_records = [Dataset(**rec).dict() for rec in records]
        if valid_records:
            dataset_collection.insert_many(valid_records)

        # Detect column types
        column_types = {col: detect_column_type(df[col]) for col in EXPECTED_COLUMNS}

        # Store metadata in a separate collection
        dataset_metadata_collection.insert_one({
            "upload_id": upload_id,
            "column_types": column_types,
            "created_at": pd.Timestamp.now().isoformat()
        })

        # Broadcast to all clients that a new dataset was uploaded
        await manager.broadcast(f"dataset_uploaded:{upload_id}")

        return {
            "message": "CSV uploaded successfully",
            "upload_id": upload_id,
            "rows_inserted": len(valid_records),
            "column_types": column_types,
            "num_duplicates": num_duplicates
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


@router.get("/all/headers")
async def get_all_headers():
    """Returns all unique headers and merged column types across all uploads"""
    records = list(dataset_collection.find({}, {"_id": 0}))
    if not records:
        raise HTTPException(status_code=404, detail="No records found")

    ignored_columns = {}

    # Collect all keys that have at least one non-null value
    valid_headers = set()
    for record in records:
        for key, value in record.items():
            if key in ignored_columns:
                continue
            if value not in (None, "", [], {}):
                valid_headers.add(key)

    # Fetch all stored metadata
    metadata_docs = list(dataset_metadata_collection.find({}, {"_id": 0, "column_types": 1}))

    # Combine column type info across uploads
    type_counts = defaultdict(Counter)

    for meta in metadata_docs:
        col_types = meta.get("column_types", {})
        for col, ctype in col_types.items():
            type_counts[col][ctype] += 1

    # Decide majority column type per column
    merged_column_types = {}
    for col in valid_headers:
        if col in type_counts:
            merged_column_types[col] = type_counts[col].most_common(1)[0][0]
        else:
            merged_column_types[col] = "unknown"

    return {
        "valid_headers": sorted(valid_headers),
        "column_types": merged_column_types,
    }




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
    """Returns headers and column types for a given upload_id"""
    query = {"upload_id": upload_id}
    records = list(dataset_collection.find(query, {"_id": 0}))

    if not records:
        raise HTTPException(status_code=404, detail="No records found")

    ignored_columns = {}

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
