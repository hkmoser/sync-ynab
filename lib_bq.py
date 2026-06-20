import time

import pandas as pd
from google.cloud import bigquery

def append_to_bigquery(data, table_id, dataset_id='home_ynab', project_id='ecstatic-pod-443723-f6'):
    """
    Appends a pandas DataFrame or CSV file to an existing BigQuery table.

    Parameters:
        data (pd.DataFrame or str): A pandas DataFrame or path to a CSV file.
        dataset_id (str): The ID of the dataset in BigQuery.
        table_id (str): The ID of the table in BigQuery.
        project_id (str): The GCP project ID.
    """
    if isinstance(data, str):
        df = pd.read_csv(data)
    elif isinstance(data, pd.DataFrame):
        df = data
    else:
        raise ValueError("Input must be a pandas DataFrame or path to a CSV file.")

    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=True
    )

    start_time = time.time()
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Waits for the job to complete
    elapsed_time = time.time() - start_time

    print(f"Appended {len(df)} rows to {dataset_id}.{table_id} in {elapsed_time:.2f} seconds")

def write_to_bigquery(data, table_id, dataset_id='home_ynab', project_id='ecstatic-pod-443723-f6'):
    """
    Writes a pandas DataFrame to an existing BigQuery table.

    Parameters:
        data (pd.DataFrame or str): A pandas DataFrame or path to a CSV file.
        dataset_id (str): The ID of the dataset in BigQuery.
        table_id (str): The ID of the table in BigQuery.
        project_id (str): The GCP project ID.
    """
    if isinstance(data, str):
        df = pd.read_csv(data)
    elif isinstance(data, pd.DataFrame):
        df = data
    else:
        raise ValueError("Input must be a pandas DataFrame or path to a CSV file.")

    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=True,
    )

    start_time = time.time()
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Waits for the job to complete
    elapsed_time = time.time() - start_time

    print(f"Wrote {len(df)} rows to {dataset_id}.{table_id} in {elapsed_time:.2f} seconds")
