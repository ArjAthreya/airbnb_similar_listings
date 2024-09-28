# airbnb_similar_listings

web service that returns similar airbnb listings to a given listing

## Development Requirements

- Python3.9 or greater
- Pip
- Poetry (Python Package Manager)

## Installation

```sh
python -m venv venv
source venv/bin/activate
make install
```

or 

```sh
conda create -n airbnb_similar_listings python=3.9
```

## Setting up .env file

Take a look at .env.example and set the correct values depending on the use case 

```
# Path to the NYC Airbnb listings dataset 
NYC_CSV_FILEPATH=/Users/arjunathreya/Projects/airbnb_similar_listings/ml/data/dataset/listings.csv

# Path of where the db file should be stored
DB_PATH=/Users/arjunathreya/Projects/airbnb_similar_listings/airbnb.db

# Clustering and embedding parameters
SENTENCE_TRANSFORMER_MODEL=distilbert-base-nli-stsb-mean-tokens
SIMILARITY_THRESHOLD=0.93
MIN_SAMPLES=2
USE_PCA=False   

# HDBSCAN if USE_PCA is True
MIN_CLUSTER_SIZE=2

```

## Creating DB and values and then running on Localhost 
`make create_db` creates the db and populates it

`make run` runs the service on localhost:8080

## Deploy app

`make deploy`

## Access Swagger Documentation

> <http://localhost:8080/docs>

## Access Redocs Documentation

> <http://localhost:8080/redoc>



