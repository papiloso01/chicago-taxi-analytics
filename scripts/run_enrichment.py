"""Compatibility entry point for the PostgreSQL workflow."""
import runpy
runpy.run_module("chicago_taxi.publishing.postgres_enrichment", run_name="__main__")
