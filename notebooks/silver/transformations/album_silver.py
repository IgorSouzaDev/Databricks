import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="silver_album",
    comment="Silver layer - Album com tipos corretos"
)
def silver_album():
    return spark.read \
        .format("delta") \
        .table("projeto_engenharia.bronze.album") \
        .select(
            F.col("AlbumId").cast("int").alias("album_id"),
            F.trim(F.col("Title")).alias("album_nome"),
            F.col("ArtistId").cast("int").alias("artist_id"),
            F.current_timestamp().alias("silver_processed_at")
        )