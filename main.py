#!/usr/bin/env python
from pyspark.sql import SparkSession

import jobs as j
from dataset import load_dataset, preprocess
from settings import DATA_PATH, OUTPUT_PATH
from utils import save_csv


def main():
    spark = SparkSession.builder.config("spark.driver.memory", "15g").appName("IMDBAnalysis").getOrCreate()

    dataset = load_dataset(DATA_PATH, spark)
    dataset = preprocess(dataset)

    # Bogdana's jobs
    # j.most_popular_actors(dataset).show()
    # j.worst_ranked_movie_genres(dataset).show()
    # j.episodic_tv_series_statictics(dataset).show()
    # j.multilingual_titles(dataset).show()
    # j.top_collaborations(dataset).show()
    # j.the_youngest_actors(dataset).show()

    # Andrii Shchur jobs
    # j.best_popular_directors(dataset).show()
    # j.best_rated_languages(dataset).show()
    # j.versatile_directors(dataset).show()
    # j.best_decades(dataset).show()
    # j.directors_with_long_movies(dataset).show()
    # j.best_genres_per_decades(dataset).show()

    # Andrii's (Marusyk) jobs
    #original_titles_languages_df = j.original_title_languages(dataset)
    #original_titles_languages_df.show()
    #save_csv(original_titles_languages_df, f"{OUTPUT_PATH}/original_titles_languages")

    #genres_avg_rating = j.genres_avg_rating(dataset)
    #genres_avg_rating.show()
    #save_csv(genres_avg_rating, f"{OUTPUT_PATH}/genres_avg_rating")

    #adult_movies_per_year = j.adult_movies_stats(dataset)
    #adult_movies_per_year.show()
    #save_csv(adult_movies_per_year, f"{OUTPUT_PATH}/adult_movies_stats")

    #directors_popular_genre = j.directors_genres(dataset)
    #directors_popular_genre.show()
    #save_csv(directors_popular_genre, f"{OUTPUT_PATH}/directors_popular_genre")

    #busy_actors = j.busy_actors(dataset)
    #busy_actors.show()
    #save_csv(busy_actors, f"{OUTPUT_PATH}/busy_actors")

    longest_tv = j.longest_tv_series(dataset)
    longest_tv.show()
    save_csv(longest_tv, f"{OUTPUT_PATH}/longest_tv")

    input("Press any button to end the program")


if __name__ == "__main__":
    main()
