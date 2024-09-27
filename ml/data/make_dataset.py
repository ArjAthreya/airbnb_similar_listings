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
    df = handle_missing_values(df)
    df = clean_price_column(df)
    return df

def select_columns(df):
    listing_df_cols = ['id', 'listing_url', 'price', 'property_type', 'room_type', 'bathrooms_text', 'bedrooms', 'beds', 'accommodates', 'latitude', 'longitude', 'neighbourhood_group_cleansed', 'neighbourhood_cleansed', 'neighborhood_overview', 'description', 'amenities', 'host_about', 'review_scores_rating', 'review_scores_cleanliness', 'review_scores_checkin', 'review_scores_communication', 'review_scores_location', 'review_scores_value', 'number_of_reviews']
    return df[listing_df_cols]

def handle_missing_values(df):
    # Drop columns with too many missing values (more than 50%)
    threshold = 0.5 * len(df)
    df_cleaned = df.dropna(thresh=threshold, axis=1)

    # Fill missing string/object columns with empty strings
    string_cols = df_cleaned.select_dtypes(include=['object']).columns
    df_cleaned.loc[:, string_cols] = df_cleaned[string_cols].fillna('')

    # Fill missing numerical columns with mean
    numeric_cols = df_cleaned.select_dtypes(include=['float64', 'int64']).columns
    df_cleaned.loc[:, numeric_cols] = df_cleaned[numeric_cols].fillna(df_cleaned[numeric_cols].mean())

    return df_cleaned

def clean_price_column(df):
    if 'price' in df.columns:
        df.loc[:, 'price'] = df['price'].replace('', np.nan)
        df.loc[:, 'price'] = df['price'].replace({'\$': '', ',': ''}, regex=True)
        df.loc[:, 'price'] = pd.to_numeric(df['price'], errors='coerce')
    return df


if __name__ == "__main__":
    main()
