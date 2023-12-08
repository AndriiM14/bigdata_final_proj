from pyspark.sql import SparkSession
from pyspark.sql.functions import split

from schemas import *

spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()


def read_imdb_data_spark():
    name_basics = spark.read.csv(
        "data/name.basics.tsv.gz", sep="\t", header=True, schema=name_basics_schema
    )
    title_akas = spark.read.csv(
        "data/title.akas.tsv.gz", sep="\t", header=True, schema=title_akas_schema
    )
    title_basics = spark.read.csv(
        "data/title.basics.tsv.gz", sep="\t", header=True, schema=title_basics_schema
    )
    title_crew = spark.read.csv(
        "data/title.crew.tsv.gz", sep="\t", header=True, schema=title_crew_schema
    )
    title_episode = spark.read.csv(
        "data/title.episode.tsv.gz", sep="\t", header=True, schema=title_episode_schema
    )
    title_principals = spark.read.csv(
        "data/title.principals.tsv.gz",
        sep="\t",
        header=True,
        schema=title_principals_schema,
    )
    title_ratings = spark.read.csv(
        "data/title.ratings.tsv.gz", sep="\t", header=True, schema=title_ratings_schema
    )

    name_basics = name_basics.withColumn(
        "primaryProfession",
        split(name_basics["primaryProfession"], ",").cast(ArrayType(StringType())),
    )
    name_basics = name_basics.withColumn(
        "knownForTitles",
        split(name_basics["knownForTitles"], ",").cast(ArrayType(StringType())),
    )
    title_akas = title_akas.withColumn(
        "types", split(title_akas["types"], ",").cast(ArrayType(StringType()))
    )
    title_akas = title_akas.withColumn(
        "attributes", split(title_akas["attributes"], ",").cast(ArrayType(StringType()))
    )
    title_basics = title_basics.withColumn(
        "genres", split(title_basics["genres"], ",").cast(ArrayType(StringType()))
    )
    title_crew = title_crew.withColumn(
        "directors", split(title_crew["directors"], ",").cast(ArrayType(StringType()))
    )
    title_crew = title_crew.withColumn(
        "writers", split(title_crew["writers"], ",").cast(ArrayType(StringType()))
    )

    return (
        name_basics,
        title_akas,
        title_basics,
        title_crew,
        title_episode,
        title_principals,
        title_ratings,
    )


def main():
    (
        name_basics_df,
        title_akas_df,
        title_basics_df,
        title_crew_df,
        title_episode_df,
        title_principals_df,
        title_ratings_df,
    ) = read_imdb_data_spark()

    name_basics_df.show()
    title_akas_df.show()
    title_basics_df.show()
    title_crew_df.show()
    title_episode_df.show()
    title_principals_df.show()
    title_ratings_df.show()


if __name__ == "__main__":
    main()
