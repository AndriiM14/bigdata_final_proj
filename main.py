#!/usr/bin/env python
from pyspark.sql import SparkSession
from dataset import load_dataset, preprocess
from settings import DATA_PATH


def main():
    spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()

    dataset = load_dataset(DATA_PATH, spark)
    dataset = preprocess(dataset)

    dataset.nbasics.printSchema()
    dataset.nbasics.show()



if __name__ == "__main__":
    main()
