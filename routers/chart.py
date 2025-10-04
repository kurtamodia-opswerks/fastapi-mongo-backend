from fastapi import APIRouter, HTTPException
from models.dataset import AggregateRequest
from db.mongo import dataset_collection

router = APIRouter()


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