# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
import hdbscan
from sklearn.cluster import DBSCAN
from tqdm import tqdm
from loguru import logger

def apply_pca(valid_vectors_df, n_components):
    """Apply PCA to reduce the dimensionality of the vector data."""
    pca = PCA(n_components='mle', svd_solver='full')
    reduced_data = pca.fit_transform(np.stack(valid_vectors_df['average_embedding'].values))
    return reduced_data

def perform_clustering_hdbscan(reduced_data):
    """Perform clustering on the dimensionality reduced data using HDBSCAN."""
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=2,
        min_samples=1,
        metric='euclidean',
        cluster_selection_epsilon=0.0
    )
    labels = clusterer.fit_predict(reduced_data)
    return labels, clusterer

def perform_clustering_dbscan(valid_vectors_df, similarity_threshold, min_samples):
    """Perform DBSCAN clustering on preprocessed data."""
    eps_value = 1 - similarity_threshold
    db = DBSCAN(eps=eps_value, min_samples=min_samples, metric='cosine')
    valid_vectors_df['cluster'] = db.fit_predict(np.stack(valid_vectors_df['average_embedding'].values))
    return valid_vectors_df

def pipeline_clustering(df, use_pca=False):
    logger.info("Start clustering process.")
    
    # Filter out rows with null embeddings
    valid_vectors_df = df[df['average_embedding'].notnull()]
    
    # Apply PCA if enabled
    if use_pca:
        n_components = 10  # Adjust based on your needs
        logger.info("Applying PCA...")
        reduced_data = apply_pca(valid_vectors_df, n_components)
    
        logger.info("Performing HDBSCAN clustering...")
        labels, clusterer = perform_clustering_hdbscan(reduced_data)
        # Add the cluster labels to the original DataFrame
        valid_vectors_df['cluster'] = labels
    else:
        valid_vectors_df = perform_clustering_dbscan(valid_vectors_df, 0.93, 2)

    
    # Group listings by cluster
    cluster_groups = valid_vectors_df.groupby('cluster')['id'].apply(list).reset_index(name='listings_in_cluster')
    
    # Convert listings_in_cluster to text
    cluster_groups['listings_in_cluster'] = cluster_groups['listings_in_cluster'].apply(lambda x: ','.join(map(str, x))).astype(str)
    # Merge cluster groups back to original DataFrame if needed
    valid_vectors_df = valid_vectors_df.merge(cluster_groups, on='cluster', how='left')
    
    print(valid_vectors_df.head(3))

    return valid_vectors_df