from fastapi import APIRouter, HTTPException
from schemas.dashboard import Dashboard, DashboardUpdate
from models.dashboard import dashboards_collection
from models.chart import charts_collection
from bson.objectid import ObjectId

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


# ================================================
# Add or update a dashboard
# ================================================
@router.post("/add")
async def add_to_dashboard(request: Dashboard):
    """
    Add a chart ID to an existing dashboard (based on mode + upload_id).
    If no dashboard exists, create a new one with this chart.
    """
    existing = dashboards_collection.find_one({
        "mode": request.mode,
        "upload_id": request.upload_id,
    })

    if existing:
        # Only add the chart if it's not already in the dashboard
        dashboards_collection.update_one(
            {"_id": existing["_id"]},
            {"$addToSet": {"charts": request.chart_id}}  
        )
        return {
            "message": "Chart added to existing dashboard successfully",
            "dashboard_id": str(existing["_id"]),
        }

    else:
        # Create a new dashboard with the chart
        new_dashboard = {
            "mode": request.mode,
            "upload_id": request.upload_id,
            "charts": [request.chart_id],
            "year_from": None,
            "year_to": None,
        }
        result = dashboards_collection.insert_one(new_dashboard)
        return {
            "message": "New dashboard created successfully",
            "dashboard_id": str(result.inserted_id),
        }


# ================================================
# Get a dashboard + populated charts
# ================================================
@router.get("/{mode}/{upload_id}")
async def get_dashboard(mode: str, upload_id: str = None):
    """
    Fetch a dashboard for a given mode and upload_id (which may be null),
    and populate chart details automatically.
    """
    query = {"mode": mode}

    # Handle cases where upload_id is "null" or "undefined" from frontend
    if upload_id in (None, "null", "undefined", ""):
        query["upload_id"] = None
    else:
        query["upload_id"] = upload_id

    dashboard = dashboards_collection.find_one(query)
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    dashboard["_id"] = str(dashboard["_id"])

    # --- Populate chart details ---
    chart_ids = dashboard.get("charts", [])
    if chart_ids:
        try:
            chart_objects = list(
                charts_collection.find({
                    "_id": {"$in": [ObjectId(cid) for cid in chart_ids if ObjectId.is_valid(cid)]}
                })
            )
            # Convert ObjectId fields to string
            for chart in chart_objects:
                chart["_id"] = str(chart["_id"])

            dashboard["charts"] = chart_objects
        except Exception:
            dashboard["charts"] = []

    return {
        **dashboard,
        "year_from": dashboard.get("year_from"),
        "year_to": dashboard.get("year_to"),
    }




# ================================================
# Delete a chart from a dashboard
# ================================================
@router.delete("/{dashboard_id}/{chart_id}")
async def delete_chart_from_dashboard(dashboard_id: str, chart_id: str):
    """Remove a specific chart ID from a dashboard"""
    dashboard = dashboards_collection.find_one({"_id": ObjectId(dashboard_id)})
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    result = dashboards_collection.update_one(
        {"_id": ObjectId(dashboard_id)},
        {"$pull": {"charts": chart_id}}
    )

    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Chart not found in dashboard")

    return {"message": f"Chart '{chart_id}' removed from dashboard"}


# ================================================
# Update dashboard date range
# ================================================
@router.put("/{dashboard_id}/date-range")
async def update_dashboard_date_range(dashboard_id: str, request: DashboardUpdate):
    """Update the year_from and year_to of a dashboard"""
    update_data = {}

    if request.year_from is not None:
        update_data["year_from"] = request.year_from
    if request.year_to is not None:
        update_data["year_to"] = request.year_to

    if not update_data:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    result = dashboards_collection.update_one(
        {"_id": ObjectId(dashboard_id)},
        {"$set": update_data}
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    return {"message": "Dashboard date range updated successfully"}

