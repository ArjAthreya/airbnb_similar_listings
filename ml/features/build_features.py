# -*- coding: utf-8 -*-
import os
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.preprocessing import normalize
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from loguru import logger
import torch
from dotenv import load_dotenv

load_dotenv()

def create_overview(row):
    # Extract relevant fields
    property_type = row['property_type']
    room_type = row['room_type']
    price = row['price']
    bathrooms = row['bathrooms_text']
    bedrooms = int(row['bedrooms']) if not pd.isnull(row['bedrooms']) else 'N/A'
    beds = int(row['beds']) if not pd.isnull(row['beds']) else 'N/A'
    accommodates = int(row['accommodates']) if not pd.isnull(row['accommodates']) else 'N/A'
    neighborhood = row['neighbourhood_cleansed']
    review_rating = row['review_scores_rating']
    
    # Construct the description
    description_text = (
        f"This {bedrooms}-bedroom {property_type} is located in {neighborhood}. "
        f"The {room_type} accommodates {accommodates} guests with {beds} bed(s) and "
        f"{bathrooms}. "
    )
    
    # Adding price details
    if not pd.isnull(price):
        description_text += f"The price per night is ${price:.2f}. "

    # Add review information if available
    if not pd.isnull(review_rating):
        description_text += (
            f"The property has a review rating of {review_rating:.1f}/5"
        )
    
    return description_text.strip()

def evaluate_listing(row):
    # Extract relevant fields
    overall_rating = row['review_scores_rating']
    cleanliness = row['review_scores_cleanliness']
    checkin = row['review_scores_checkin']
    communication = row['review_scores_communication']
    location = row['review_scores_location']
    value = row['review_scores_value']
    description = row['description'] if not pd.isnull(row['description']) else "No detailed description available."

    # Start the outline with the property description
    outline = f"Property Description: {description} Review Overview:"
    
    # Evaluate cleanliness
    if cleanliness >= 4.5:
        outline += "• The property is consistently rated highly for cleanliness."
    elif cleanliness >= 3.5:
        outline += "• The property is generally clean but could use some improvement."
    else:
        outline += "• Cleanliness is often highlighted as a concern by guests."
    
    # Evaluate the check-in experience
    if checkin >= 4.5:
        outline += "• The check-in process is rated as smooth and easy by most guests."
    elif checkin >= 3.5:
        outline += "• The check-in process is generally okay, but there might be occasional issues."
    else:
        outline += "• Guests frequently report issues with the check-in process."
    
    # Evaluate communication
    if communication >= 4.5:
        outline += "• The host is highly responsive and easy to communicate with."
    elif communication >= 3.5:
        outline += "• Communication with the host is generally fine, with some areas for improvement."
    else:
        outline += "• Guests have often faced difficulties in communicating with the host."
    
    # Evaluate the location
    if location >= 4.5:
        outline += "• The location is highly rated by guests, with many finding it convenient."
    elif location >= 3.5:
        outline += "• The location is generally good but may not be ideal for everyone."
    else:
        outline += "• The location might not be convenient or desirable for many guests."
    
    # Evaluate the value for money
    if value >= 4.5:
        outline += "• Guests believe the property offers excellent value for money."
    elif value >= 3.5:
        outline += "• The property offers reasonable value, though some guests may feel it's a bit pricey."
    else:
        outline += "• Guests feel the property does not offer good value for the price."
    
    # Compare overall rating and scores
    if (overall_rating >= 4.5 and cleanliness >= 4.5 and checkin >= 4.5 
        and communication >= 4.5 and location >= 4.5 and value >= 4.5):
        outline += "Overall, the reviews suggest that the property consistently meets or exceeds expectations."
    else:
        outline += "There are some areas where guest experiences may not fully align with the description, particularly in the aspects highlighted above."
        
    return outline

def construct_high_level_overview(row):
    property_desc = f"This is a {row['property_type']}."

    if isinstance(row['description'], str):
        property_desc += f" {row['description']}"
    
    if isinstance(row['neighborhood_overview'], str):
        property_desc += f" The neighborhood is described as: {row['neighborhood_overview']}"
    
    return property_desc

def generate_embeddings_with_progress(df, column_name, model, batch_size=32):
    embeddings = []
    tqdm.pandas(desc=f"Generating embeddings for {column_name}")

    for i in tqdm(range(0, len(df), batch_size), desc="Batch Encoding"):
        batch = df[column_name].iloc[i:i + batch_size].tolist()
        batch_embeddings = model.encode(batch, convert_to_tensor=True)
        embeddings.extend(batch_embeddings.tolist())

    # Normalize the embeddings
    normalized_embeddings = normalize(np.array(embeddings), norm='l2', axis=1)
    
    df[f'{column_name}_embedding'] = normalized_embeddings.tolist()
    return df

def weighted_average_embedding(embeddings, weights):
    embeddings = [np.array(embedding) for embedding in embeddings]
    weighted_sum = np.sum([embedding * weight for embedding, weight in zip(embeddings, weights)], axis=0)
    weighted_avg_embedding = weighted_sum / np.sum(weights)
    
    # Normalize the weighted average embedding
    normalized_avg_embedding = normalize(weighted_avg_embedding.reshape(1, -1), norm='l2', axis=1)[0]
    
    return normalized_avg_embedding

def apply_weighted_average(df, weight_outline=0.5, weight_description=0.3, weight_overview=0.2):
    weights = [weight_outline, weight_description, weight_overview]

    tqdm.pandas(desc="Calculating weighted average embeddings")
    
    df['average_embedding'] = df.progress_apply(
        lambda row: weighted_average_embedding(
            [row['property_outline_embedding'], row['description_summary_embedding'], row['high_level_overview_embedding']],
            weights
        ), axis=1
    )
    
    return df

def pipeline(df):
    logger.info("Start building features.")
    
    # Create overviews and outlines
    df['description_summary'] = df.apply(create_overview, axis=1)
    df['property_outline'] = df.apply(evaluate_listing, axis=1)
    df['high_level_overview'] = df.apply(construct_high_level_overview, axis=1)
    
    # Load the Sentence Transformer model
    model = SentenceTransformer(os.getenv('SENTENCE_TRANSFORMER_MODEL'))
    device = torch.device('mps' if torch.backends.mps.is_available() else 'cpu')
    model.to(device)
    
    # Generate embeddings
    df = generate_embeddings_with_progress(df, 'property_outline', model)
    df = generate_embeddings_with_progress(df, 'description_summary', model)
    df = generate_embeddings_with_progress(df, 'high_level_overview', model)
    
    # Calculate weighted average embeddings
    df = apply_weighted_average(df)
    
    logger.info("Finished building features.")
    return df
