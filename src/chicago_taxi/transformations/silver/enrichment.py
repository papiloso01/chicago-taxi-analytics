"""Left joins at unique area/local-hour grain preserve trip counts and totals."""
from pyspark.sql import functions as F, types as T
AREA_SCHEMA=T.StructType([T.StructField('community',T.StringType())])
WEATHER_SCHEMA=T.StructType([T.StructField('local_hour',T.StringType()),*[T.StructField(k,T.DoubleType()) for k in ['temperature_2m','precipitation','snowfall']]])


def build_dimensions(area_raw,weather_raw):
    areas=area_raw.select('area_id',F.from_json('payload',AREA_SCHEMA).alias('p'),F.get_json_object('payload','$.the_geom').alias('geometry_geojson')).select('area_id',F.col('p.community').alias('area_name'),'geometry_geojson')
    weather=weather_raw.select(F.from_json('payload',WEATHER_SCHEMA).alias('p')).select('p.*').withColumn('weather_local_hour',F.col('local_hour'))
    hours=weather.groupBy('weather_local_hour').agg(F.count('*').alias('source_hours'),*[F.max(k).alias(k) for k in ['temperature_2m','precipitation','snowfall']])
    hours=hours.withColumn('weather_ambiguous',F.col('source_hours')>1)
    for k in ['temperature_2m','precipitation','snowfall']:hours=hours.withColumn(k,F.when(~F.col('weather_ambiguous'),F.col(k)))
    return areas, hours
