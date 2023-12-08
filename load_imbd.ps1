 #Create directory if it doesn't exist
 New-Item -ItemType Directory -Force -Path ".\data"

# Remove all files in the data directory
Remove-Item -Path ".\data\*" -Force

# Change to the data directory
Set-Location -Path ".\data"

# Download files
Invoke-WebRequest -Uri "https://datasets.imdbws.com/name.basics.tsv.gz" -OutFile "name.basics.tsv.gz"
Invoke-WebRequest -Uri "https://datasets.imdbws.com/title.akas.tsv.gz" -OutFile "title.akas.tsv.gz"
Invoke-WebRequest -Uri "https://datasets.imdbws.com/title.basics.tsv.gz" -OutFile "title.basics.tsv.gz"
Invoke-WebRequest -Uri "https://datasets.imdbws.com/title.crew.tsv.gz" -OutFile "title.crew.tsv.gz"
Invoke-WebRequest -Uri "https://datasets.imdbws.com/title.episode.tsv.gz" -OutFile "title.episode.tsv.gz"
Invoke-WebRequest -Uri "https://datasets.imdbws.com/title.principals.tsv.gz" -OutFile "title.principals.tsv.gz"
Invoke-WebRequest -Uri "https://datasets.imdbws.com/title.ratings.tsv.gz" -OutFile "title.ratings.tsv.gz"
 
# Change back to the parent directory
Set-Location -Path ".."

