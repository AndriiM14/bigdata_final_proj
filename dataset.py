from pyspark.sql import DataFrame as df
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import ArrayType, StringType
from dataclasses import dataclass
import schemas as s


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


def split_transform(dataset: Dataset) -> Dataset:
    dataset.nbasics = split_columns(dataset.nbasics, ["primaryProfession", "knownForTitles"])
    dataset.takas = split_columns(dataset.takas, ["types", "attributes"])
    dataset.tbasics = split_columns(dataset.tbasics, ["genres"])
    dataset.tcrew = split_columns(dataset.tcrew, ["directors", "writers"])

    return dataset


def preprocess(dataset: Dataset) -> Dataset:
    dataset = split_transform(dataset)

    return dataset

