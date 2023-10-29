from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t

def test_env():
    spark_session = (SparkSession.builder
                        .master('local')
                        .appName('Env test')
                        .config(conf=SparkConf())
                        .getOrCreate())
    
    dummy_data = [['Artur', 32], ['Lisa', 21]]
    dummy_schema = t.StructType([
        t.StructField('Name', t.StringType()),
        t.StructField('Age', t.ByteType())
    ])

    dummy_df = spark_session.createDataFrame(dummy_data, dummy_schema)

    dummy_df.show()


def main():
    test_env()


if __name__ == '__main__':
    main()