from fastapi import APIRouter, HTTPException
from schemas.chart import AggregateRequest, Chart
from models.dataset import dataset_collection
from models.chart import charts_collection
from bson.objectid import ObjectId

router = APIRouter(prefix="/chart", tags=["Chart"])

@router.post("/aggregate")
async def aggregate(request: AggregateRequest):
    """Returns aggregated data based on the provided request parameters"""
    funcs = {"sum": "$sum", "avg": "$avg", "count": "$sum", "min": "$min", "max": "$max"}
    if request.agg_func not in funcs:
        raise HTTPException(status_code=400, detail=f"Invalid agg_func. Choose from {list(funcs.keys())}")

    match_stage = {}
    if request.upload_id:
        match_stage["upload_id"] = request.upload_id

    if request.year_from or request.year_to:
        match_stage["year"] = {}
        if request.year_from:
            match_stage["year"]["$gte"] = int(request.year_from)
        if request.year_to:
            match_stage["year"]["$lte"] = int(request.year_to)

    pipeline = [
        {"$match": match_stage},
        {"$group": {
            "_id": f"${request.x_axis}",
            request.y_axis: ({"$sum": 1} if request.agg_func == "count" else {funcs[request.agg_func]: f"${request.y_axis}"})
        }},
        {"$project": {request.x_axis: "$_id", request.y_axis: f"${request.y_axis}", "_id": 0}},
        {"$sort": {request.x_axis: 1}}
    ]
    result = list(dataset_collection.aggregate(pipeline))
    if not result:
        raise HTTPException(status_code=404, detail="No records found")
    return result


@router.post("/save")
async def save_chart(request: Chart):
    """Saves the chart data to the database"""
    result = charts_collection.insert_one(request.dict())
    return {"message": "Chart saved successfully", "chart_id": str(result.inserted_id)}


@router.get("/saved/{upload_id}")
async def get_saved_charts(upload_id: str):
    """Returns all saved charts for a given upload_id"""
    charts = list(charts_collection.find({"upload_id": upload_id}, {"_id": 1, "name": 1, "chart_type": 1, "x_axis": 1, "y_axis": 1, "agg_func": 1, "year_from": 1, "year_to": 1}))
    for chart in charts:
        chart["_id"] = str(chart["_id"])
    return charts


@router.get("/saved/chart/{chart_id}")
async def get_chart(chart_id: str):
    """Returns a specific saved chart"""
    chart = charts_collection.find_one({"_id": ObjectId(chart_id)})
    if not chart:
        raise HTTPException(status_code=404, detail="Chart not found")
    chart["_id"] = str(chart["_id"])
    return chart
