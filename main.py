#!/usr/bin/env python
from pyspark.sql import SparkSession

import jobs as j
from dataset import load_dataset, preprocess
from settings import DATA_PATH, OUTPUT_PATH


def main():
    spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()

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
    j.versatile_directors(dataset).show()

    input("Press any button to end the program")


if __name__ == "__main__":
    main()
