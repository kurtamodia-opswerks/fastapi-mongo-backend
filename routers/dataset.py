import uuid
import pandas as pd
from fastapi import APIRouter, UploadFile, File, HTTPException
from models.dataset import Dataset
from db.mongo import dataset_collection
from serializers.dataset import all_data

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


@router.get("/{upload_id}/contents")
async def get_datasets(upload_id: str):
    """Fetch datasets (optionally by upload_id)"""
    query = {"upload_id": upload_id} if upload_id else {}
    records = list(dataset_collection.find(query, {"_id": 0}))
    if not records:
        raise HTTPException(status_code=404, detail="No records found")
    return all_data(records)


@router.get("/{upload_id}/headers")
async def get_headers(upload_id: str):
    """Return headers that have at least one valid value"""
    query = {"upload_id": upload_id} if upload_id else {}
    records = list(dataset_collection.find(query, {"_id": 0}))
    if not records:
        raise HTTPException(status_code=404, detail="No records found")
    df = pd.DataFrame(records)
    return {"valid_headers": [col for col in df.columns if df[col].notnull().any()]}

