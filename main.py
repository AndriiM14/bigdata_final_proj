#!/usr/bin/env python
from pyspark.sql import SparkSession
from dataset import load_dataset, preprocess
from settings import DATA_PATH, OUTPUT_PATH
from utils import save_csv


def main():
    spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()

    dataset = load_dataset(DATA_PATH, spark)
    dataset = preprocess(dataset)

    dataset.tratings.printSchema()
    dataset.tratings.show()
    save_csv(dataset.tratings, f"{OUTPUT_PATH}/tratings")


    input("Press any button to end the program")



if __name__ == "__main__":
    main()
