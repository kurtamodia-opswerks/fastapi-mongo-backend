from fastapi import APIRouter, HTTPException
from schemas.dashboard import Dashboard
from models.dashboard import dashboards_collection
from bson.objectid import ObjectId

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])

# ================================================
# Add or update a dashboard
# ================================================
@router.post("/add")
async def add_to_dashboard(request: Dashboard):
    """
    Adds charts to a dashboard. If a dashboard with the same mode and upload_id exists, it updates it.
    Otherwise, it creates a new dashboard.
    """
    existing = dashboards_collection.find_one({"mode": request.mode, "upload_id": request.upload_id})

    if existing:
        # Merge charts without duplicates (based on chart name)
        existing_chart_names = {c.get("name") for c in existing.get("charts", [])}
        new_charts = [chart.dict() for chart in request.charts if chart.name not in existing_chart_names]

        if new_charts:
            dashboards_collection.update_one(
                {"_id": existing["_id"]},
                {"$push": {"charts": {"$each": new_charts}}}
            )
        return {"message": "Dashboard updated successfully", "dashboard_id": str(existing["_id"])}
    else:
        result = dashboards_collection.insert_one(request.dict())
        return {"message": "Dashboard created successfully", "dashboard_id": str(result.inserted_id)}
    


# ================================================
# Get a dashboard
# ================================================
@router.get("/{mode}/{upload_id}")
async def get_dashboard(mode: str, upload_id: str):
    """Fetch a dashboard for a given mode and upload_id"""
    dashboard = dashboards_collection.find_one({"mode": mode, "upload_id": upload_id})
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    dashboard["_id"] = str(dashboard["_id"])
    return dashboard

# ================================================
# Delete a chart from a dashboard
# ================================================
@router.delete("/{dashboard_id}/{chart_name}")
async def delete_chart_from_dashboard(dashboard_id: str, chart_name: str):
    """Remove a specific chart from a dashboard by chart name"""
    dashboard = dashboards_collection.find_one({"_id": ObjectId(dashboard_id)})
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    result = dashboards_collection.update_one(
        {"_id": ObjectId(dashboard_id)},
        {"$pull": {"charts": {"name": chart_name}}}
    )

    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Chart not found in dashboard")

    return {"message": f"Chart '{chart_name}' removed from dashboard"}
