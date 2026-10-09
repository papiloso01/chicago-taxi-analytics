"""Legacy table identifiers retained for PostgreSQL migration compatibility."""
from chicago_taxi.transformations.silver.taxi_trips import build_warehouse
from chicago_taxi.transformations.gold.taxi_marts import build_marts

def build_models(bronze):
    models = build_warehouse(bronze)
    models.update(build_marts(models["silver.fct_trips"]))
    return models
