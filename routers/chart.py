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

    # Match Stage
    match_stage = {}
    if request.upload_id:
        match_stage["upload_id"] = request.upload_id

    if request.year_from or request.year_to:
        match_stage["year"] = {}
        if request.year_from:
            match_stage["year"]["$gte"] = int(request.year_from)
        if request.year_to:
            match_stage["year"]["$lte"] = int(request.year_to)

    # # Detect continuous fields
    # sample_docs = list(dataset_collection.find(match_stage, {request.x_axis: 1, request.y_axis: 1}).limit(100))

    # def is_continuous(field):
    #     """Heuristic: numeric and has many unique values"""
    #     values = [d.get(field) for d in sample_docs if isinstance(d.get(field), (int, float))]
    #     if not values:
    #         return False
    #     unique_count = len(set(values))
    #     return unique_count > 15  

    # x_is_continuous = is_continuous(request.x_axis)
    # y_is_continuous = is_continuous(request.y_axis)

    # # Aggregate
    # if x_is_continuous and y_is_continuous:
    #     # Both continuous: use bucketAuto for trend data
    #     pipeline = [
    #         {"$match": match_stage},
    #         {
    #             "$bucketAuto": {
    #                 "groupBy": f"${request.x_axis}",
    #                 "buckets": 20,
    #                 "output": {
    #                     f"avg_{request.y_axis}": {"$avg": f"${request.y_axis}"},
    #                     "count": {"$sum": 1},
    #                 },
    #             }
    #         },
    #         {"$project": {
    #             "x_range_min": "$_id.min",
    #             "x_range_max": "$_id.max",
    #             f"avg_{request.y_axis}": 1,
    #             "count": 1,
    #             "_id": 0
    #         }},
    #         {"$sort": {"x_range_min": 1}}
    #     ]

    # elif x_is_continuous:
    #     # Continuous x, categorical y: bucket x and aggregate y
    #     pipeline = [
    #         {"$match": match_stage},
    #         {
    #             "$bucketAuto": {
    #                 "groupBy": f"${request.x_axis}",
    #                 "buckets": 20,
    #                 "output": {
    #                     f"{request.y_axis}": (
    #                         {"$sum": 1}
    #                         if request.agg_func == "count"
    #                         else {funcs[request.agg_func]: f"${request.y_axis}"}
    #                     ),
    #                     "count": {"$sum": 1},
    #                 },
    #             }
    #         },
    #         {"$project": {
    #             "x_range_min": "$_id.min",
    #             "x_range_max": "$_id.max",
    #             f"{request.y_axis}": 1,
    #             "count": 1,
    #             "_id": 0
    #         }},
    #         {"$sort": {"x_range_min": 1}}
    #     ]

    # else:
    # Default: categorical grouping
    pipeline = [
        {"$match": match_stage},
        {"$group": {
            "_id": f"${request.x_axis}",
            request.y_axis: (
                {"$sum": 1}
                if request.agg_func == "count"
                else {funcs[request.agg_func]: f"${request.y_axis}"}
            )
        }},
        {"$project": {request.x_axis: "$_id", request.y_axis: f"${request.y_axis}", "_id": 0}},
        {"$sort": {request.x_axis: 1}},
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


@router.put("/update/{chart_id}")
async def update_chart(chart_id: str, request: Chart):
    """Updates an existing chart by its ID"""
    try:
        obj_id = ObjectId(chart_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid chart ID")

    # Check if chart exists
    existing_chart = charts_collection.find_one({"_id": obj_id})
    if not existing_chart:
        raise HTTPException(status_code=404, detail="Chart not found")

    # Update only provided fields (non-null ones)
    update_data = {k: v for k, v in request.dict().items() if v is not None}

    result = charts_collection.update_one(
        {"_id": obj_id},
        {"$set": update_data}
    )

    if result.modified_count == 0:
        return {"message": "No changes made to chart"}

    return {"message": "Chart updated successfully", "chart_id": chart_id}


@router.get("/saved/all")
async def get_all_saved_charts():
    """Returns all saved charts"""
    charts = list(charts_collection.find({"mode": "aggregated"}, {"_id": 1, "name": 1, "chart_type": 1, "x_axis": 1, "y_axis": 1, "agg_func": 1, "year_from": 1, "year_to": 1}))
    for chart in charts:
        chart["_id"] = str(chart["_id"])
    return charts


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

@router.delete("/delete/{chart_id}")
async def remove_chart(chart_id: str):
    charts_collection.delete_one({"_id": ObjectId(chart_id)})
    return {"message": "Chart deleted successfully"}    


# --- Year Range ---
@router.get("/year-range")
async def get_year_range(upload_id: str | None = None):
    """Returns the minimum and maximum year values available in the dataset"""
    match_stage = {}
    if upload_id:
        match_stage["upload_id"] = upload_id

    pipeline = [
        {"$match": match_stage},
        {
            "$group": {
                "_id": None,
                "min_year": {"$min": "$year"},
                "max_year": {"$max": "$year"}
            }
        },
        {"$project": {"_id": 0, "min_year": 1, "max_year": 1}}
    ]

    result = list(dataset_collection.aggregate(pipeline))

    if not result or result[0].get("min_year") is None:
        raise HTTPException(status_code=404, detail="No year data found")

    return result[0]
