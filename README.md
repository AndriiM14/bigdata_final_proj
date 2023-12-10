# NULP BigData final project

### Team:
 - Bogdana Gonserovska
 - Andrii Schur
 - Anna Horbova
 - Andrii Marusyk

### Dataset  - [IMDB](https://developer.imdb.com/non-commercial-datasets/)

### Download dataset
```
# UNIX
./load_imbd.sh

# Windows
./load_imbd.ps1
```

### Data/Outputs location
```
./data # for data folder
./outputs # for your outputs
```

### Example of creating dataset in code
```
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
```

### Branch naming convention
```
businesslg-<your initials>

Example:
businesslg-am
```

### Where to put your jobs
##### Write separate functions in jobs.py and import them and use in main.py
jobs.py
```
def some_job(d: Dataset) -> None:
    ...
```
main.py
```
...
from jobs import some_job

def main():
    datast = load_dataset(...)
    ...
    some_job(dataset)
```

### Code style:
 - snake_case
 - double quotes
 - no * imports
 - use column variables to access columns
