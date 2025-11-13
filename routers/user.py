from fastapi import APIRouter, HTTPException
from schemas.user import User
from models.user import user_collection
from bson.objectid import ObjectId

router = APIRouter(prefix="/users", tags=["User"])

@router.post("/sync")
async def sync_user(user: User):
    print(user)
    try:
        existing = user_collection.find_one({"email": user.email})
        if not existing:
            user_collection.insert_one(user.dict())
        return {"status": "ok"}
    except Exception as e:
        return {"status": e}
        
    