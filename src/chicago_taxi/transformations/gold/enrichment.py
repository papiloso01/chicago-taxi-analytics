from pyspark.sql import functions as F
from chicago_taxi.transformations.silver.enrichment import build_dimensions

def build_enrichment(fact, area_raw, weather_raw):
    areas, hours = build_dimensions(area_raw, weather_raw)
    enriched=fact
    for role in ['pickup','dropoff']:
        key=role+'_community_area';normalized=role+'_area_id'
        enriched=enriched.withColumn(normalized,F.col(key).cast('int').cast('string'))
        dim=areas.select(F.col('area_id').alias(normalized),F.col('area_name').alias(role+'_area_name'))
        enriched=enriched.join(F.broadcast(dim),normalized,'left').withColumn(role+'_area_name',F.coalesce(F.col(role+'_area_name'),F.lit('Unknown')))
    enriched=enriched.withColumn('weather_local_hour',F.concat(F.col('trip_date').cast('string'),F.lit('T'),F.lpad(F.col('trip_hour').cast('string'),2,'0'),F.lit(':00:00'))).join(hours,'weather_local_hour','left')
    enriched=enriched.withColumn('weather_available',F.col('temperature_2m').isNotNull() & F.col('precipitation').isNotNull() & F.col('snowfall').isNotNull())
    enriched=enriched.withColumn('weather_ambiguous',F.coalesce(F.col('weather_ambiguous'),F.lit(False)))
    enriched=enriched.withColumn('weather_condition',F.when(F.col('weather_ambiguous'),'Ambiguous DST hour').when(~F.col('weather_available'),'Unavailable').when(F.col('snowfall')>0,'Snow').when(F.col('precipitation')>0,'Wet').otherwise('Dry'))
    summary=enriched.groupBy('trip_date','trip_hour','weather_condition').agg(F.count('*').alias('trips'),F.sum('trip_total').alias('reported_total_usd'),F.avg('trip_seconds').alias('average_trip_seconds'),F.max('temperature_2m').alias('temperature_c'),F.max('precipitation').alias('precipitation_mm'),F.max('snowfall').alias('snowfall_cm'))
    pickups=enriched.select(F.col('pickup_area_id').alias('area_id'),F.col('pickup_area_name').alias('area_name'),'trip_date',F.lit('Pickup').alias('activity'),'trip_total')
    dropoffs=enriched.select(F.col('dropoff_area_id').alias('area_id'),F.col('dropoff_area_name').alias('area_name'),'trip_date',F.lit('Dropoff').alias('activity'),'trip_total')
    demand=pickups.unionByName(dropoffs).groupBy('trip_date','area_id','area_name','activity').agg(F.count('*').alias('trips'),F.sum('trip_total').alias('reported_total_usd'))
    return {'silver.dim_community_area':areas,'silver.dim_weather_hour':hours,'gold.mart_enriched_trips':enriched,'gold.mart_weather_demand':summary,'gold.mart_area_demand':demand}
