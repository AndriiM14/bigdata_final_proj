from pyspark.sql import DataFrame as df, SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import ArrayType, StringType
from dataclasses import dataclass
from typing import Callable
import schemas as s
from utils import camel_to_snake


@dataclass
class Dataset():
    nbasics: df
    takas: df
    tbasics: df
    tcrew: df
    tepisode: df
    tprincipals: df
    tratings: df


def load_dataset(path: str, spark: SparkSession) -> Dataset:
    name_basics_df = spark.read.csv(
        f"{path}/name.basics.tsv.gz", sep="\t", header=True, schema=s.name_basics_schema
    )
    title_akas_df = spark.read.csv(
        f"{path}/title.akas.tsv.gz", sep="\t", header=True, schema=s.title_akas_schema
    )
    title_basics_df = spark.read.csv(
        f"{path}/title.basics.tsv.gz", sep="\t", header=True, schema=s.title_basics_schema
    )
    title_crew_df = spark.read.csv(
        f"{path}/title.crew.tsv.gz", sep="\t", header=True, schema=s.title_crew_schema
    )
    title_episode_df = spark.read.csv(
        f"{path}/title.episode.tsv.gz", sep="\t", header=True, schema=s.title_episode_schema
    )
    title_principals_df = spark.read.csv(
        f"{path}/title.principals.tsv.gz",
        sep="\t",
        header=True,
        schema=s.title_principals_schema,
    )
    title_ratings_df = spark.read.csv(
        f"{path}/title.ratings.tsv.gz", sep="\t", header=True, schema=s.title_ratings_schema
    )

    return Dataset(
        name_basics_df,
        title_akas_df,
        title_basics_df,
        title_crew_df,
        title_episode_df,
        title_principals_df,
        title_ratings_df
    )


def split_columns(dataframe: df, columns: list[str], separator: str = ",") -> df:
    for column in columns:
        dataframe = dataframe.withColumn(
            column,
            split(dataframe[column], separator).cast(ArrayType(StringType()))
        )

    return dataframe


def format_columns(dataframe: df, formater: Callable[[str], str]) -> df:
    for column in  dataframe.columns:
        dataframe = dataframe.withColumnRenamed(column, formater(column))

    return dataframe


def split_transform(dataset: Dataset) -> Dataset:
    dataset.nbasics = split_columns(dataset.nbasics, ["primaryProfession", "knownForTitles"])
    dataset.takas = split_columns(dataset.takas, ["types", "attributes"])
    dataset.tbasics = split_columns(dataset.tbasics, ["genres"])
    dataset.tcrew = split_columns(dataset.tcrew, ["directors", "writers"])

    return dataset


def column_names_transform(dataset: Dataset) -> Dataset:
    dataset.nbasics = format_columns(dataset.nbasics, camel_to_snake)
    dataset.takas = format_columns(dataset.takas, camel_to_snake)
    dataset.tbasics = format_columns(dataset.tbasics, camel_to_snake)
    dataset.tcrew = format_columns(dataset.tcrew, camel_to_snake)
    dataset.tepisode = format_columns(dataset.tepisode, camel_to_snake)
    dataset.tprincipals = format_columns(dataset.tprincipals, camel_to_snake)
    dataset.tratings = format_columns(dataset.tratings, camel_to_snake)


def preprocess(dataset: Dataset) -> Dataset:
    dataset = split_transform(dataset)
    datast = column_names_transform(dataset)

    return dataset

