from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, explode, max, split

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


"""
Most popular actors:
Who are the top 10 actors known for the highest number of titles, and what are their primary professions?
"""


def most_popular_actors(name_basics):
    name_basics_exploded = name_basics.select(
        "nconst",
        "primaryName",
        "primaryProfession",
        explode("knownForTitles").alias("knownTitle"),
    )
    name_basics_exploded = name_basics_exploded.select(
        "nconst",
        "primaryName",
        "knownTitle",
        explode("primaryProfession").alias("primary_profession"),
    )

    actors_only = name_basics_exploded.filter(col("primary_profession") == "actor")

    titles_count = actors_only.groupBy(
        "nconst", "primaryName", "primary_profession"
    ).agg(count("*").alias("title_count"))

    titles_count_ordered = titles_count.orderBy(col("title_count").desc())

    top_10_actors = titles_count_ordered.limit(10)

    return top_10_actors


"""
Worst Ranked Movie Genres:
Question: What are the worst 5 genres based on the average ratings?
"""


def worst_ranked_movie_genres(title_basics, title_ratings):
    joined_df = title_basics.join(title_ratings, "tconst")
    exploded_df = joined_df.select(
        "tconst", explode("genres").alias("genre"), "averageRating"
    )
    filtered_df = exploded_df.filter(col("genre") != r"\N")

    average_ratings_by_genre = filtered_df.groupBy("genre").agg(
        avg("averageRating").alias("average_rating")
    )

    sorted_df = average_ratings_by_genre.orderBy("average_rating")

    worst_5_genres = sorted_df.limit(5)

    return worst_5_genres


"""
Episodic TV Series Statistics:
Question: For episodic TV series, which TV series has the highest number of episodes?
"""


def episodic_tv_series_statictics(title_episode):
    max_episodes_series = (
        title_episode.groupBy("parentTconst")
        .agg(max("episodeNumber").alias("max_episodes"))
        .orderBy("max_episodes", ascending=False)
        .limit(1)
    )

    return max_episodes_series


"""
Multilingual Movie Titles:
Question: Titles with the largest number of translations
"""


def multilingual_titles(title_akas):
    multilingual_titles_count = title_akas.groupBy("titleId", "title").agg(
        count("language").alias("language_count")
    )

    multilingual_titles_count_top_10 = multilingual_titles_count.orderBy(
        "language_count", ascending=False
    ).limit(10)

    return multilingual_titles_count_top_10


"""
Top Directors and Writers Collaborations:
Question: What are the top 5 director-writer collaborations with the highest average ratings for their movies?
"""


def top_collaborations(title_crew, title_ratings, name_basics):
    exploded_directors_df = title_crew.select(
        "tconst", "writers", explode("directors").alias("director")
    )
    exploded_writers_df = exploded_directors_df.select(
        "tconst", "director", explode("writers").alias("writer")
    )
    filter_non_null_directors = exploded_writers_df.filter(col("director") != r"\N")
    filter_non_null_writers = filter_non_null_directors.filter(col("writer") != r"\N")

    crew_ratings_joined = filter_non_null_writers.join(title_ratings, "tconst")

    collaboration_names_df = (
        crew_ratings_joined.join(name_basics, col("director") == col("nconst"), "left")
        .withColumnRenamed("primaryName", "directorName")
        .drop("nconst")
        .join(name_basics, col("writer") == col("nconst"), "left")
        .withColumnRenamed("primaryName", "writerName")
        .drop("nconst")
    )

    collaboration_ratings = collaboration_names_df.groupBy(
        "directorName", "writerName", "tconst", "director", "writer"
    ).agg(avg("averageRating").alias("average_rating"))

    return collaboration_ratings.orderBy(col("average_rating").desc()).limit(5)


"""
The youngest actors:
Question: Who are the youngest actors?
"""


def the_youngest_actors(name_basics):
    professions_exploded = name_basics.select(
        "birthYear", "primaryName", explode("primaryProfession").alias("profession")
    )
    young_actors = (
        professions_exploded.filter(col("profession") == "actor")
        .orderBy("birthYear", ascending=False)
        .select("primaryName", "birthYear")
    )

    return young_actors


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

    # Bohdana's queries

    # most_popular_actors(name_basics_df).show()
    # worst_ranked_movie_genres(title_basics_df, title_ratings_df).show()
    # episodic_tv_series_statictics(title_episode_df).show()
    # multilingual_titles(title_akas_df).show()
    # top_collaborations(title_crew_df, title_ratings_df, name_basics_df).show()
    # the_youngest_actors(name_basics_df).show()


if __name__ == "__main__":
    main()
