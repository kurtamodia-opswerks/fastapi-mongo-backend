import pandas as pd
from fastapi import APIRouter, UploadFile, File, HTTPException
from lib.utils import generate_short_uuid, _create_row_hash, _get_columns_from_schema
from models.parquet import parquet_collection

from schemas.parquet import ChartDataRequest

router = APIRouter(prefix="/parquet", tags=["Parquet"])

# --- Main Upload Endpoint ---

@router.post("/upload")
async def upload_parquet(file: UploadFile = File(...)):
    """
    Handles Parquet upload, checks for duplicates, and saves new dataset.
    If an identical dataset is uploaded, it returns the existing upload_id.
    """
    try:
        if not file.filename.endswith('.parquet'):
            raise HTTPException(status_code=400, detail="Invalid file type. Please upload a .parquet file.")

        df = pd.read_parquet(file.file)

        # Drop duplicates within the uploaded file itself
        initial_rows = len(df)
        df.drop_duplicates(inplace=True)
        duplicates_in_file = initial_rows - len(df)

        if df.empty:
            return {
                "message": "File is empty or all rows were duplicates within the file.",
                "upload_id": None,
                "rows_inserted": 0,
                "duplicates_found_in_file": duplicates_in_file,
                "duplicates_found_in_db": 0,
                "columns": []
            }

        # Check for duplicates against the database using a content hash
        df['_hash'] = df.apply(_create_row_hash, axis=1)
        
        existing_hashes = {doc['_hash'] for doc in parquet_collection.find({}, {"_hash": 1}) if '_hash' in doc}
        
        df_new = df[~df['_hash'].isin(existing_hashes)]
        duplicates_in_db = len(df) - len(df_new)

        # Handle different upload scenarios
        # Scenario A: All rows in the file are duplicates of existing ones in the DB
        if df_new.empty:
            print("[DEBUG] All rows are duplicates of existing DB records. Checking for a common upload_id.")
            
            # Find all upload_ids associated with the hashes from the uploaded file
            duplicate_hashes = list(df['_hash'])
            matching_docs = parquet_collection.find(
                {'_hash': {'$in': duplicate_hashes}},
                {'_id': 0, 'upload_id': 1}
            )
            found_ids = {doc['upload_id'] for doc in matching_docs if 'upload_id' in doc}

            # If all duplicates belong to a SINGLE previous upload, return that ID
            if len(found_ids) == 1:
                existing_upload_id = found_ids.pop()
                first_doc = parquet_collection.find_one({"upload_id": existing_upload_id})
                columns = _get_columns_from_schema(first_doc) if first_doc else []
                
                print(f"[DEBUG] Found a single matching upload_id: {existing_upload_id}")
                return {
                    "message": "This file is an exact duplicate of a previous upload.",
                    "upload_id": existing_upload_id,
                    "rows_inserted": 0,
                    "duplicates_found_in_file": duplicates_in_file,
                    "duplicates_found_in_db": duplicates_in_db,
                    "columns": columns,
                    "status": "duplicate"
                }
            else:
                # This is the "mix tape" scenario.
                print(f"[DEBUG] Found {len(found_ids)} matching upload_ids. No single source.")
                return {
                    "message": "File contains a mix of records from multiple existing datasets. No new data was inserted.",
                    "upload_id": None,
                    "rows_inserted": 0,
                    "duplicates_found_in_file": duplicates_in_file,
                    "duplicates_found_in_db": duplicates_in_db,
                    "columns": [col for col in df.columns if col != '_hash']
                }

        # Scenario B: There are new, unique rows to insert
        else:
            upload_id = generate_short_uuid()
            records = df_new.to_dict(orient="records")

            for record in records:
                record["upload_id"] = upload_id

            print(f"[DEBUG] Attempting to insert {len(records)} new records with upload_id: {upload_id}")
            parquet_collection.insert_many(records)
            print("[DEBUG] Successfully inserted new records.")

            return {
                "message": "Parquet file processed successfully.",
                "upload_id": upload_id,
                "rows_inserted": len(records),
                "duplicates_found_in_file": duplicates_in_file,
                "duplicates_found_in_db": duplicates_in_db,
                "columns": [col for col in df.columns if col != '_hash']
            }

    except Exception as e:
        print(f"[DEBUG] An exception occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")
    


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
