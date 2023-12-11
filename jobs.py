import pyspark.sql.functions as f
from pyspark.sql import DataFrame as df

import columns as c
from dataset import Dataset

POPULAR_ACTORS_LIMIT = 10
WORST_GENRES_LIMIT = 5
HIGHEST_EPISODES_SERIES_LIMIT = 1
MULTILINGUAL_TITLES_LIMIT = 10
TOP_COLLABORATIONS_LIMIT = 10

POPULAR_DIRECTOR_MIN_VOTES = 50000
ACTIVE_DIRECTOR_MIN_TITLE_COUNT = 5
ACTIVE_LANGUAGE_MIN_TITLE_COUNT = 10000


def example_job(d: Dataset) -> None:
    d.tratings.groupBy(c.average_rating).count().show()


def most_popular_actors(d: Dataset) -> df:
    """
    Most popular actors:
    Who are the top 10 actors known for the highest number of titles, and what are their primary professions?
    """

    name_basics_exploded = d.nbasics.select(
        c.nconst,
        c.primary_name,
        c.primary_profession,
        f.explode(c.known_for_titles).alias(c.known_title),
    )
    name_basics_exploded = name_basics_exploded.select(
        c.nconst,
        c.primary_name,
        c.known_title,
        f.explode(c.primary_profession).alias(c.primary_profession),
    )

    actors_only = name_basics_exploded.filter(f.col(c.primary_profession) == "actor")

    titles_count = actors_only.groupBy(c.nconst, c.primary_name, c.primary_profession).agg(
        f.count("*").alias(c.title_count)
    )

    titles_count_ordered = titles_count.orderBy(f.col(c.title_count).desc())

    top_10_actors = titles_count_ordered.limit(POPULAR_ACTORS_LIMIT)

    return top_10_actors


def worst_ranked_movie_genres(d: Dataset) -> df:
    """
    Worst Ranked Movie Genres:
    Question: What are the worst 5 genres based on the average ratings?
    """

    joined_df = d.tbasics.join(d.tratings, c.tconst)
    exploded_df = joined_df.select(c.tconst, f.explode(c.genres).alias(c.genre), c.average_rating)
    filtered_df = exploded_df.filter(f.col(c.genre) != r"\N")

    average_ratings_by_genre = filtered_df.groupBy(c.genre).agg(f.avg(c.average_rating).alias(c.average_rating))

    sorted_df = average_ratings_by_genre.orderBy(c.average_rating)

    worst_5_genres = sorted_df.limit(WORST_GENRES_LIMIT)

    return worst_5_genres


def episodic_tv_series_statistics(d: Dataset) -> df:
    """
    Episodic TV Series Statistics:
    Question: For episodic TV series, which TV series has the highest number of episodes?
    """

    max_episodes_series = (
        d.tepisode.groupBy(c.parent_tconst)
        .agg(f.max(c.episode_number).alias(c.max_episodes))
        .orderBy(c.max_episodes, ascending=False)
        .limit(HIGHEST_EPISODES_SERIES_LIMIT)
    )

    return max_episodes_series


def multilingual_titles(d: Dataset) -> df:
    """
    Multilingual Movie Titles:
    Question: Titles with the largest number of translations
    """

    multilingual_titles_count = d.takas.groupBy(c.title_id, c.title).agg(f.count(c.language).alias(c.language_count))

    multilingual_titles_count_top_10 = multilingual_titles_count.orderBy(c.language_count, ascending=False).limit(
        MULTILINGUAL_TITLES_LIMIT
    )

    return multilingual_titles_count_top_10


def top_collaborations(d: Dataset) -> df:
    """
    Top Directors and Writers Collaborations:
    Question: What are the top 5 director-writer collaborations with the highest average ratings for their movies?
    """

    exploded_directors_df = d.tcrew.select(c.tconst, c.writers, f.explode(c.directors).alias(c.director))
    exploded_writers_df = exploded_directors_df.select(c.tconst, c.director, f.explode(c.writers).alias(c.writer))
    filter_non_null_directors = exploded_writers_df.filter(f.col(c.director) != r"\N")
    filter_non_null_writers = filter_non_null_directors.filter(f.col(c.writer) != r"\N")

    crew_ratings_joined = filter_non_null_writers.join(d.tratings, c.tconst)

    collaboration_names_df = (
        crew_ratings_joined.join(d.nbasics, f.col(c.director) == f.col(c.nconst), "left")
        .withColumnRenamed(c.primary_name, c.director_name)
        .drop(c.nconst)
        .join(d.nbasics, f.col(c.writer) == f.col(c.nconst), "left")
        .withColumnRenamed(c.primary_name, c.writer_name)
        .drop(c.nconst)
    )

    collaboration_ratings = collaboration_names_df.groupBy(
        c.director_name, c.writer_name, c.tconst, c.director, c.writer
    ).agg(f.avg(c.average_rating).alias(c.average_rating))

    return collaboration_ratings.orderBy(f.col(c.average_rating).desc()).limit(TOP_COLLABORATIONS_LIMIT)


def the_youngest_actors(d: Dataset) -> df:
    """
    The youngest actors:
    Question: Who are the youngest actors?
    """

    professions_exploded = d.nbasics.select(
        c.birth_year,
        c.primary_name,
        f.explode(c.primary_profession).alias(c.profession),
    )
    young_actors = (
        professions_exploded.filter(f.col(c.profession) == "actor")
        .orderBy(c.birth_year, ascending=False)
        .select(c.primary_name, c.birth_year)
    )

    return young_actors


def best_popular_directors(d: Dataset) -> df:
    """
    Best popular directors:
    Question: Who are the best active popular directors?
    """

    joined_df = d.tcrew.select(c.tconst, f.explode(c.directors).alias(c.director)).join(d.tratings, c.tconst)
    name_basics_df = d.nbasics.select(c.nconst, c.primary_name)
    directors = joined_df.join(name_basics_df, f.col(c.director) == f.col(c.nconst), "left")
    popular_directors = (
        directors.select(c.average_rating, c.num_votes, c.primary_name)
        .groupBy(c.primary_name)
        .agg(
            f.avg(c.average_rating).alias(c.average_rating),
            f.max(c.num_votes).alias("max_votes"),
            f.count("*").alias("titles_count"),
        )
        .filter(f.col("max_votes") > POPULAR_DIRECTOR_MIN_VOTES)
        .filter(f.col("titles_count") > ACTIVE_DIRECTOR_MIN_TITLE_COUNT)
        .orderBy(c.average_rating, ascending=False)
    )

    return popular_directors


def best_rated_languages(d: Dataset) -> df:
    """
    Best rated languages
    Question: In which languages were the best rated movies filmed on average?
    """

    return (
        d.takas.select(c.title_id, c.language)
        .filter(f.col(c.language) != r"\N")
        .join(d.tratings.select(c.tconst, c.average_rating), f.col(c.title_id) == f.col(c.tconst))
        .groupBy(c.language)
        .agg(f.avg(c.average_rating).alias(c.average_rating), f.count("*").alias("titles_count"))
        .filter(f.col("titles_count") > ACTIVE_LANGUAGE_MIN_TITLE_COUNT)
        .orderBy(c.average_rating, ascending=False)
    )


def versatile_directors(d: Dataset) -> df:
    """
    The most versatile directors
    Question: What directors have worked on the highest number of genres?
    """

    _genres_set_col = "genres_set"
    _genres_set_size_col = "genres_set_size"

    directors_genres_df = (
        d.tcrew.select(c.tconst, f.explode(c.directors).alias(c.director))
        .filter(f.col(c.director) != r"\N")
        .join(d.tbasics.select(c.tconst, f.explode(c.genres).alias(c.genre)), c.tconst)
        .groupBy(c.director)
        .agg(f.collect_set(c.genre).alias(_genres_set_col))
    )

    # FYI: Alan Smithee is an official pseudonym used by film directors who wish to disown a project
    return (
        directors_genres_df.join(
            d.nbasics.select(c.nconst, c.primary_name),
            (f.col(c.director) == f.col(c.nconst)) & (f.col(c.primary_name) != "Alan Smithee"),
        )
        .select(c.primary_name, _genres_set_col, f.size(_genres_set_col).alias(_genres_set_size_col))
        .orderBy(f.col(_genres_set_size_col), ascending=False)
    )


def best_decades(d: Dataset) -> df:
    """
    Best rated decades:
    Question: In which decades were the best rated movies filmed on average?
    """

    _decade_col = "decade"

    return (
        d.tbasics.filter(f.col(c.start_year).isNotNull())
        .withColumn(_decade_col, f.expr(f"floor({c.start_year}/10)*10"))
        .join(d.tratings.select(c.tconst, c.average_rating), c.tconst)
        .groupBy(_decade_col)
        .agg(f.avg(c.average_rating).alias(c.average_rating))
        .orderBy(c.average_rating, ascending=False)
    )


def directors_with_long_movies(d: Dataset) -> df:
    """
    Directors that film the longest movies:
    Question: Which directors filmed the longest movies?
    """

    _avg_minutes_col = "avg_minutes"

    return (
        d.tcrew.select(c.tconst, f.explode(c.directors).alias(c.director))
        .filter(f.col(c.director) != r"\N")
        .join(
            d.tbasics.select(c.tconst, c.runtime_minutes).filter(
                (f.col(c.runtime_minutes).isNotNull()) & (f.col(c.title_type) == "movie")
            ),
            c.tconst,
        )
        .join(d.nbasics.select(c.nconst, c.primary_name), f.col(c.director) == f.col(c.nconst))
        .select(c.primary_name, c.runtime_minutes)
        .groupBy(c.primary_name)
        .agg(f.avg(c.runtime_minutes).alias(_avg_minutes_col))
        .orderBy(_avg_minutes_col, ascending=False)
    )


def best_genres_per_decades(d: Dataset) -> df:
    """
    The highest rated genres throughout the decades
    Question: What genres were rated the highest in each decade?
    """

    _decade_col = "decade"
    _avg_avg_rating_col = "avg_avg_rating"

    return (
        d.tbasics.select(c.tconst, c.start_year, f.explode(c.genres).alias(c.genre))
        .filter(f.col(c.start_year).isNotNull() & (f.col(c.genre) != r"\N"))
        .withColumn(_decade_col, f.expr(f"floor({c.start_year}/10)*10"))
        .join(d.tratings.select(c.tconst, c.average_rating), c.tconst)
        .groupBy(_decade_col, c.genre)
        .agg(f.avg(c.average_rating).alias(_avg_avg_rating_col))
        .orderBy(_avg_avg_rating_col, ascending=False)
        .groupBy(_decade_col)
        .agg(f.max(_avg_avg_rating_col).alias(_avg_avg_rating_col), f.first(c.genre).alias(c.genre))
        .orderBy(_decade_col, ascending=False)
    )
