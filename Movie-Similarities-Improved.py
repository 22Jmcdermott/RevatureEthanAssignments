import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, LongType, StringType

MOVIES_PATH  = "file:///home/mcdja/projects/spark/data_engineering/spark/ml-100k/u.item"
RATINGS_PATH = "file:///home/mcdja/projects/spark/data_engineering/spark/ml-100k/u.data"
OUTPUT_PATH  = "file:///home/mcdja/projects/spark/data_engineering/spark/output/movie-sims-100k.parquet"

spark = SparkSession.builder.appName("MovieSimilarities100k").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Loading movie names...")
movies_df = (
    spark.read.text(MOVIES_PATH)
    .select(F.split(F.col("value"), r"\|").alias("parts"))
    .select(
        F.col("parts")[0].cast(IntegerType()).alias("movie_id"),
        F.col("parts")[1].cast(StringType()).alias("title"),
    )
    .na.drop(subset=["movie_id", "title"])
    .cache()
)

print("Loading ratings...")
ratings_df = (
    spark.read.text(RATINGS_PATH)
    .select(F.split(F.col("value"), r"\t").alias("parts"))
    .select(
        F.col("parts")[0].cast(IntegerType()).alias("user_id"),
        F.col("parts")[1].cast(IntegerType()).alias("movie_id"),
        F.col("parts")[2].cast(DoubleType()).alias("rating"),
        F.col("parts")[3].cast(LongType()).alias("timestamp"),
    )
    .na.drop(subset=["user_id", "movie_id", "rating"])
    .cache()
)

left  = ratings_df.select("user_id", F.col("movie_id").alias("movie1"), F.col("rating").alias("rating1"))
right = ratings_df.select("user_id", F.col("movie_id").alias("movie2"), F.col("rating").alias("rating2"))
pairs_df = left.join(right, on="user_id").where(F.col("movie1") < F.col("movie2"))

stats_df = pairs_df.groupBy("movie1", "movie2").agg(
    F.count(F.lit(1)).alias("numPairs"),
    F.sum(F.col("rating1") * F.col("rating1")).alias("sum_xx"),
    F.sum(F.col("rating2") * F.col("rating2")).alias("sum_yy"),
    F.sum(F.col("rating1") * F.col("rating2")).alias("sum_xy"),
)
denom = F.sqrt(F.col("sum_xx")) * F.sqrt(F.col("sum_yy"))

similarities_df = (
    stats_df
    .select(
        "movie1", "movie2",
        F.when(denom != 0, F.col("sum_xy") / denom).otherwise(F.lit(0.0)).alias("score"),
        "numPairs",
    )
    .cache()
)

print(f"Saving similarities to Parquet: {OUTPUT_PATH}")
similarities_df.write.mode("overwrite").parquet(OUTPUT_PATH)

if len(sys.argv) > 1:
    movieID               = int(sys.argv[1])
    scoreThreshold        = 0.97
    coOccurrenceThreshold = 10

    filtered_df = (
        similarities_df
        .where(
            ((F.col("movie1") == movieID) | (F.col("movie2") == movieID))
            & (F.col("score")    > scoreThreshold)
            & (F.col("numPairs") > coOccurrenceThreshold)
        )
        .withColumn(
            "similar_movie_id",
            F.when(F.col("movie1") == movieID, F.col("movie2")).otherwise(F.col("movie1"))
        )
        .join(
            movies_df.select(F.col("movie_id").alias("similar_movie_id"), "title"),
            on="similar_movie_id", how="left",
        )
        .orderBy(F.col("score").desc())
        .limit(10)
    )

    target_row   = movies_df.where(F.col("movie_id") == movieID).select("title").first()
    target_title = target_row["title"] if target_row else str(movieID)

    print(f"\nTop 10 similar movies for: {target_title}")
    for row in filtered_df.collect():
        print(row["title"], "\tscore:", round(float(row["score"]), 4), "\tstrength:", int(row["numPairs"]))

spark.stop()
