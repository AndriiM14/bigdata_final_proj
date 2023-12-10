from dataset import Dataset
from pyspark.sql import DataFrame as df
import columns as c


def example_job(d: Dataset) -> None:
    d.tratings.groupBy(c.average_rating).count().show()


