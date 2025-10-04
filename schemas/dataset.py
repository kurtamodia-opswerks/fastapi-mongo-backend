def individual_data(dataset):
    return {
        "upload_id": dataset.get("upload_id"),
        "row_id": dataset.get("row_id"),
        "model": dataset.get("model"),
        "year": dataset.get("year"),
        "region": dataset.get("region"),
        "color": dataset.get("color"),
        "transmission": dataset.get("transmission"),
        "mileage_km": dataset.get("mileage_km"),
        "price_usd": dataset.get("price_usd"),
        "sales_volume": dataset.get("sales_volume"),
    }

def all_data(datasets):
    return [individual_data(dataset) for dataset in datasets]
