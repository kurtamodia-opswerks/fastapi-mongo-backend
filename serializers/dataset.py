def individual_data(data):
    return {
        "upload_id": data.get("upload_id"),
        "row_id": data.get("row_id"),
        "model": data.get("model"),
        "year": data.get("year"),
        "region": data.get("region"),
        "color": data.get("color"),
        "transmission": data.get("transmission"),
        "mileage_km": data.get("mileage_km"),
        "price_usd": data.get("price_usd"),
        "sales_volume": data.get("sales_volume"),
    }

def all_data(dataset):
    return [individual_data(data) for data in dataset]
