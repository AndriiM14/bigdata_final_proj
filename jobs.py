from pyspark.sql.functions import col, avg, explode, count
from pyspark.sql.functions import max as max_

from dataset import Dataset
from pyspark.sql import DataFrame as df
import columns as c


def example_job(d: Dataset) -> None:
    d.tratings.groupBy(c.average_rating).count().show()


def best_popular_directors_by_avg_movie_rating(d: Dataset):
    title_ratings, title_crew, name_basics = d.tratings, d.tcrew, d.nbasics
    joined_df = title_crew.select(
        "tconst", explode("directors").alias("director")
    ).join(title_ratings, "tconst")
    name_basics_df = name_basics.select("nconst", "primary_name")
    data = joined_df.join(name_basics_df, col("director") == col("nconst"), "left")
    data = (
        data.select("average_rating", "num_votes", "primary_name")
        .groupBy("primary_name")
        .agg(
            avg("average_rating").alias("average_rating"),
            max_("num_votes").alias("max_votes"),
            count("*").alias("title_count"),
        )
        .filter(col("max_votes") > 50000)
        .filter(col("title_count") > 5)
        .orderBy("average_rating", ascending=False)
    )

    data.show()
