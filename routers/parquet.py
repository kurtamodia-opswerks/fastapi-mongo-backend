import pandas as pd
from fastapi import APIRouter, UploadFile, File, HTTPException
from lib.utils import generate_short_uuid
from models.parquet import parquet_collection
import hashlib
import json

from schemas.parquet import ChartDataRequest

router = APIRouter(prefix="/parquet", tags=["Parquet"])

# Upload Parquet
@router.post("/upload")
async def upload_parquet(file: UploadFile = File(...)):
    """Handles Parquet upload, checks for duplicates, and saves new dataset"""
    try:
        print("[DEBUG] Entered upload_parquet function.")
        if not file.filename.endswith('.parquet'):
            print(f"[DEBUG] File validation failed: {file.filename}")
            raise HTTPException(status_code=400, detail="Invalid file type. Please upload a .parquet file.")
        
        print(f"[DEBUG] File validation successful for: {file.filename}")

        print("[DEBUG] Attempting to read parquet file with pandas...")
        df = pd.read_parquet(file.file)
        print(f"[DEBUG] Successfully read parquet file. DataFrame shape: {df.shape}")

        # --- Duplicate Handling Logic ---

        # Drop duplicates within the uploaded file itself
        initial_rows = len(df)
        df.drop_duplicates(inplace=True)
        duplicates_in_file = initial_rows - len(df)
        print(f"[DEBUG] Dropped {duplicates_in_file} duplicates from within the uploaded file.")

        # Check for duplicates against the database using a content hash
        duplicates_in_db = 0
        if not df.empty:
            # Function to create a consistent hash for a row
            def create_row_hash(row):
                serialized_row = json.dumps(row.to_dict(), sort_keys=True, default=str)
                return hashlib.sha256(serialized_row.encode()).hexdigest()

            df['_hash'] = df.apply(create_row_hash, axis=1)

            print("[DEBUG] Fetching existing record hashes from the database.")
            existing_hashes = {doc['_hash'] for doc in parquet_collection.find({}, {"_hash": 1}) if '_hash' in doc}
            print(f"[DEBUG] Found {len(existing_hashes)} existing hashes.")

            original_record_count = len(df)
            df = df[~df['_hash'].isin(existing_hashes)]
            duplicates_in_db = original_record_count - len(df)
            print(f"[DEBUG] Found {duplicates_in_db} records that already exist in the DB. They will be ignored.")
        
        # --- End of Duplicate Handling ---

        # Generate a unique ID for this upload
        upload_id = generate_short_uuid()

        # Get records from the de-duplicated dataframe
        records = df.to_dict(orient="records")

        # Add upload_id to each new record before insertion
        for record in records:
            record["upload_id"] = upload_id

        if records:
            print(f"[DEBUG] Attempting to insert {len(records)} new records into MongoDB...")
            parquet_collection.insert_many(records)
            print("[DEBUG] Successfully inserted new records.")

        # Get the list of columns to return, excluding the internal hash
        columns_to_return = [col for col in df.columns if col != '_hash']

        return {
            "message": "Parquet file processed successfully.",
            "upload_id": upload_id,
            "rows_inserted": len(records),
            "duplicates_found_in_file": duplicates_in_file,
            "duplicates_found_in_db": duplicates_in_db,
            "columns": columns_to_return
        }

    except Exception as e:
        print(f"[DEBUG] An exception occurred: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    


@router.post("/chart-data")
async def fetch_chart_data(request: ChartDataRequest):
    """
    Fetches specific chart data from the parquet collection for a given upload_id.
    """
    try:
        print(f"[DEBUG] Entered fetch_chart_data function for upload_id: {request.upload_id}")
        query = {"upload_id": request.upload_id}
        
        # Define the fields to be returned from the database
        projection = {
            "_id": 0,  # Exclude the default MongoDB '_id'
            "_hash": 0, # Exclude hash
        }

        print(f"[DEBUG] Querying 'parquet_collection' with query: {query}")
        
        # Find all documents and only include the specified fields
        cursor = parquet_collection.find(query, projection)
        
        # Convert the cursor to a list of documents
        records = list(cursor)
        
        print(f"[DEBUG] Found {len(records)} records.")

        return records

    except Exception as e:
        print(f"[DEBUG] An exception occurred in fetch_chart_data: {e}")
        raise HTTPException(status_code=500, detail="An error occurred while fetching chart data.")
