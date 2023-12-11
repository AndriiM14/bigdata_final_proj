from dataset import Dataset
from pyspark.sql import DataFrame as df
from pyspark.sql.window import Window
import columns as c
import pyspark.sql.functions as f


POPULAR_ACTORS_LIMIT = 10
WORST_GENRES_LIMIT = 5
HIGHEST_EPISODES_SERIES_LIMIT = 1
MULTILINGUAL_TITLES_LIMIT = 10
TOP_COLOBORATIONS_LIMIT = 10
ORIGINAL_TITLE_TRUE = 1
IS_ADULT_TRUE = 1
ADULT_MIN_YEAR = 2000
ADULT_MAX_YEAR = 2023
NONE_VALUE = r"\N"


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

    titles_count = actors_only.groupBy(
        c.nconst, c.primary_name, c.primary_profession
    ).agg(f.count("*").alias(c.title_count))

    titles_count_ordered = titles_count.orderBy(f.col(c.title_count).desc())

    top_10_actors = titles_count_ordered.limit(POPULAR_ACTORS_LIMIT)

    return top_10_actors


def worst_ranked_movie_genres(d: Dataset) -> df:
    """
    Worst Ranked Movie Genres:
    Question: What are the worst 5 genres based on the average ratings?
    """

    joined_df = d.tbasics.join(d.tratings, c.tconst)
    exploded_df = joined_df.select(
        c.tconst, f.explode(c.genres).alias(c.genre), c.average_rating
    )
    filtered_df = exploded_df.filter(f.col(c.genre) != r"\N")

    average_ratings_by_genre = filtered_df.groupBy(c.genre).agg(
        f.avg(c.average_rating).alias(c.average_rating)
    )

    sorted_df = average_ratings_by_genre.orderBy(c.average_rating)

    worst_5_genres = sorted_df.limit(WORST_GENRES_LIMIT)

    return worst_5_genres


def episodic_tv_series_statictics(d: Dataset) -> df:
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

    multilingual_titles_count = d.takas.groupBy(c.title_id, c.title).agg(
        f.count(c.language).alias(c.language_count)
    )

    multilingual_titles_count_top_10 = multilingual_titles_count.orderBy(
        c.language_count, ascending=False
    ).limit(MULTILINGUAL_TITLES_LIMIT)

    return multilingual_titles_count_top_10


def top_collaborations(d: Dataset) -> df:
    """
    Top Directors and Writers Collaborations:
    Question: What are the top 5 director-writer collaborations with the highest average ratings for their movies?
    """

    exploded_directors_df = d.tcrew.select(
        c.tconst, c.writers, f.explode(c.directors).alias(c.director)
    )
    exploded_writers_df = exploded_directors_df.select(
        c.tconst, c.director, f.explode(c.writers).alias(c.writer)
    )
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

    return collaboration_ratings.orderBy(f.col(c.average_rating).desc()).limit(TOP_COLOBORATIONS_LIMIT)


def the_youngest_actors(d: Dataset) -> df:
    """
    The youngest actors:
    Question: Who are the youngest actors?
    """

    professions_exploded = d.nbasics.select(
        c.birth_year, c.primary_name, f.explode(c.primary_profession).alias(c.profession)
    )
    young_actors = (
        professions_exploded.filter(f.col(c.profession) == "actor")
        .orderBy(c.birth_year, ascending=False)
        .select(c.primary_name, c.birth_year)
    )

    return young_actors


def original_title_languages(d: Dataset) -> df:
    """
    Original titles languages:
    Question: find number of original titles for each language
    """

    original_titles_df = (d.takas
        .filter(f.col(c.is_original_title) == ORIGINAL_TITLE_TRUE)
        .filter(f.col(c.language) != NONE_VALUE))

    original_titles_df.show()

    languages_count = (original_titles_df
            .groupby(c.language)
            .count()
            .withColumnRenamed("count", c.titles_count)
            .sort(f.desc(c.titles_count)))

    return languages_count


def genres_avg_rating(d: Dataset) -> df:
    """
    Genres statistics:
    Question: display average rating by genre along with average value for all movies
    """

    all_rows = Window.partitionBy()
    genre_partition = Window.partitionBy(c.genre)

    basics_ratings_df = d.tbasics.join(d.tratings, c.tconst)
    basics_ratings_df = basics_ratings_df.withColumn(c.titles_avg_rating, f.avg(c.average_rating).over(all_rows))
    basics_ratings_df = (basics_ratings_df.select(
        c.tconst,
        c.primary_title,
        f.explode(c.genres).alias(c.genre),
        c.average_rating,
        c.titles_avg_rating))

    basics_ratings_df = (basics_ratings_df
                         .withColumn(c.genre_avg_rating, f.avg(c.average_rating).over(genre_partition)))

    basics_ratings_df = basics_ratings_df.orderBy(c.average_rating, ascending=False)

    return basics_ratings_df


def adult_movies_stats(d: Dataset) -> df:
    """
    Adult movies:
    Question: find the number of adult movies for each year from 2000 to 2023 and the average rating for adult movies per year.
    """

    basics_rartings_df = d.tbasics.join(d.tratings, c.tconst)
    adult_titles_df = basics_rartings_df.filter(f.col(c.is_adult) == IS_ADULT_TRUE)
    adult_titles_df = adult_titles_df.filter((f.col(c.start_year) >= ADULT_MIN_YEAR) & (f.col(c.start_year) <= ADULT_MAX_YEAR))

    return (adult_titles_df
                .groupby(c.start_year)
                .agg(f.count(c.tconst).alias(c.year_count), f.mean(c.average_rating).alias(c.year_avg_rating))
                .orderBy(c.start_year, ascending=False))

