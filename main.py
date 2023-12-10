#!/usr/bin/env python
from pyspark.sql import SparkSession
from dataset import load_dataset, preprocess
from settings import DATA_PATH, OUTPUT_PATH
from jobs import example_job, best_popular_directors_by_avg_movie_rating


def main():
    spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()

    dataset = load_dataset(DATA_PATH, spark)
    dataset = preprocess(dataset)

    example_job(dataset)
    best_popular_directors_by_avg_movie_rating(dataset)

    input("Press any button to end the program")


if __name__ == "__main__":
    main()
