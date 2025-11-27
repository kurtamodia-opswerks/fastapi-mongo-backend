import pandas as pd
from fastapi import APIRouter, UploadFile, File, HTTPException
from models.parquet import parquet_collection

router = APIRouter(prefix="/parquet", tags=["Parquet"])

# Upload Parquet
@router.post("/upload")
async def upload_parquet(file: UploadFile = File(...)):
    """Handles Parquet upload and saves dataset"""
    try:
        print("[DEBUG] Entered upload_parquet function.")
        # Check if the uploaded file is a parquet file
        if not file.filename.endswith('.parquet'):
            print(f"[DEBUG] File validation failed: {file.filename}")
            raise HTTPException(status_code=400, detail="Invalid file type. Please upload a .parquet file.")
        
        print(f"[DEBUG] File validation successful for: {file.filename}")

        print("[DEBUG] Attempting to read parquet file with pandas...")
        df = pd.read_parquet(file.file)
        print(f"[DEBUG] Successfully read parquet file. DataFrame shape: {df.shape}")

        print("[DEBUG] Attempting to convert DataFrame to dictionary...")
        records = df.to_dict(orient="records")
        print(f"[DEBUG] Successfully converted to dictionary. Number of records: {len(records)}")


        if records:
            print("[DEBUG] Attempting to insert records into MongoDB...")
            parquet_collection.insert_many(records)
            print("[DEBUG] Successfully inserted records into MongoDB.")

        return {
            "message": "Parquet file uploaded successfully",
            "rows_inserted": len(records),
            "columns": list(df.columns)
        }

    except Exception as e:
        print(f"[DEBUG] An exception occurred: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    
@router.get("/chart-data")
async def fetch_chart_data():
    """
    Fetches specific chart data from the parquet collection.
    """
    try:
        print("[DEBUG] Entered fetch_chart_data function.")
        
        # Define the fields to be returned from the database
        projection = {
            "_id": 0  # Exclude the default MongoDB '_id'
        }

        print(f"[DEBUG] Querying 'parquet_collection' with projection: {projection}")
        
        # Find all documents and only include the specified fields
        cursor = parquet_collection.find({}, projection)
        
        # Convert the cursor to a list of documents
        records = list(cursor)
        
        print(f"[DEBUG] Found {len(records)} records.")

        return records

    except Exception as e:
        print(f"[DEBUG] An exception occurred in fetch_chart_data: {e}")
        raise HTTPException(status_code=500, detail="An error occurred while fetching chart data.")
