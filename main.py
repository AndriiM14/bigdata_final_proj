from pyspark.sql import SparkSession
from dataset import load_dataset, preprocess



def main():
    spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()

    dataset = load_dataset("./data", spark)
    dataset = preprocess(dataset)

    dataset.nbasics.printSchema()
    dataset.nbasics.show()



if __name__ == "__main__":
    main()
