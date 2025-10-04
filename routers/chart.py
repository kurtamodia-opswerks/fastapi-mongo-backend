from fastapi import APIRouter, HTTPException
from models.chart import AggregateRequest, ChartSaveRequest
from db.mongo import dataset_collection
from db.mongo import charts_collection
from bson.objectid import ObjectId

router = APIRouter()

# Aggregate dataset and return results
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


# Save chart configuration
@router.post("/save")
async def save_chart(request: ChartSaveRequest):
    chart_doc = request.dict()
    result = charts_collection.insert_one(chart_doc)
    return {"message": "Chart saved successfully", "chart_id": str(result.inserted_id)}


# Fetch all saved charts for a specific dataset
@router.get("/saved/{upload_id}")
async def get_saved_charts(upload_id: str):
    charts = list(charts_collection.find({"upload_id": upload_id}, {"_id": 1, "name": 1, "chart_type": 1, "x_axis": 1, "y_axis": 1, "agg_func": 1, "year_from": 1, "year_to": 1}))
    for chart in charts:
        chart["_id"] = str(chart["_id"])
    return charts


# Fetch single chart configuration by chart_id
@router.get("/saved/chart/{chart_id}")
async def get_chart(chart_id: str):
    chart = charts_collection.find_one({"_id": ObjectId(chart_id)})
    if not chart:
        raise HTTPException(status_code=404, detail="Chart not found")
    chart["_id"] = str(chart["_id"])
    return chart