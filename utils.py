import re

from pyspark.sql import DataFrame as df


def camel_to_snake(string: str) -> str:
    string  = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', string).lower()


def save_csv(dataframe: df, path: str) -> None:
    dataframe.write.csv(path, mode="overwrite")

