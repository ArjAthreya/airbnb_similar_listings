# -*- coding: utf-8 -*-
import os
import click
from pathlib import Path

from loguru import logger
import pandas as pd
import numpy as np

dirname = os.path.dirname(__file__)


@click.command()
@click.argument("input_filepath", default=os.path.join(dirname, 'dataset/listings.csv'), type=click.Path(exists=True))
@click.argument("output_filepath", default=os.path.join(dirname, 'dataset/cleaned_listings.csv'), type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data into cleaned data ready 
    to be analyzed.
    """
    logger.info(f"Read from {input_filepath}, write to {output_filepath}.")

    logger.info(f"Reading file...")
    df = pd.read_csv(input_filepath)

    logger.info(f"Cleaning data...")
    df_cleaned = clean_data(df)
    
    logger.info(f"Saving cleaned data...")
    df_cleaned.to_csv(output_filepath, index=False)
    logger.info(f"Cleaned data saved to {output_filepath}")

def clean_data(df):
    df = select_columns(df)
    df = remove_non_numeric_ids(df)
    df = handle_missing_values(df)
    df = clean_price_column(df)
    df = handle_missing_price(df)
    df = convert_data_match_schema(df)
    logger.info(df.isnull().sum())
    return df

def select_columns(df):
    # select columns that are needed for the schema and model
    listing_df_cols = ['id', 'listing_url', 'price', 'property_type', 'room_type', 'neighborhood_overview', 'bathrooms_text', 'bedrooms', 'beds', 'accommodates', 'latitude', 'longitude', 'neighbourhood_group_cleansed', 'neighbourhood_cleansed', 'neighborhood_overview', 'description', 'amenities', 'host_about', 'review_scores_rating', 'review_scores_cleanliness', 'review_scores_checkin', 'review_scores_communication', 'review_scores_location', 'review_scores_value', 'number_of_reviews']
    return df[listing_df_cols]

def handle_missing_values(df):
    # Drop columns with too many missing values (more than 50%)
    threshold = 0.5 * len(df)
    df_cleaned = df.dropna(thresh=threshold, axis=1)

    # Fill missing string/object columns with empty strings
    string_cols = df_cleaned.select_dtypes(include=['object']).columns
    for col in string_cols:
        df_cleaned[col] = df_cleaned[col].fillna('N/A')

    # Fill missing numerical columns with mean, except 'bedrooms' and 'beds'
    numeric_cols = df_cleaned.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
        if col == 'bedrooms' or col == 'beds':
            df_cleaned[col] = df_cleaned[col].fillna(0)
        else:
            df_cleaned[col] = df_cleaned[col].fillna(df_cleaned[col].mean())

    # Fill missing numerical columns with mean (for simplicity)
    numeric_cols = df_cleaned.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
        df_cleaned[col] = df_cleaned[col].fillna(df_cleaned[col].mean())

    return df_cleaned

def clean_price_column(df):
    # remove $ and , from price column
    if 'price' in df.columns:
        df.loc[:, 'price'] = df['price'].replace('', np.nan)
        df.loc[:, 'price'] = df['price'].replace({'\$': '', ',': ''}, regex=True)
        df.loc[:, 'price'] = pd.to_numeric(df['price'], errors='coerce')
        df.loc[:, 'price'] = df['price'].astype(float)
    return df

def remove_non_numeric_ids(df):
    # Remove rows where 'id' is not numeric
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df = df.dropna(subset=['id'])
    df['id'] = df['id'].astype(int)
    return df

def handle_missing_price(df):
    # fill missing price with average price
    average_price = df['price'].mean()
    df['price'] = df['price'].fillna(average_price)
    return df

def convert_data_match_schema(df):
    # confirm data types to match the schema
    df['id'] = df['id'].astype(int)
    df['price'] = df['price'].astype(float)
    df['bedrooms'] = df['bedrooms'].astype(float)
    df['beds'] = df['beds'].astype(float)
    df['accommodates'] = df['accommodates'].astype(int)
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    df['review_scores_rating'] = df['review_scores_rating'].astype(float)
    df['review_scores_cleanliness'] = df['review_scores_cleanliness'].astype(float)
    df['review_scores_checkin'] = df['review_scores_checkin'].astype(float)
    df['review_scores_communication'] = df['review_scores_communication'].astype(float)
    df['review_scores_location'] = df['review_scores_location'].astype(float)
    df['review_scores_value'] = df['review_scores_value'].astype(float)
    df['number_of_reviews'] = df['number_of_reviews'].astype(int)
    return df


if __name__ == "__main__":
    main()
