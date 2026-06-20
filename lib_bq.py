import time

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

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

def append_daily_snapshot(data, table_id, snapshot_date, snapshot_column='snapshot_date',
                          dataset_id='home_ynab', project_id='ecstatic-pod-443723-f6'):
    """
    Appends a dated daily snapshot to a history table.

    Before appending, any rows already present for ``snapshot_date`` are deleted
    so that re-running on the same day replaces that day's rows rather than
    creating duplicates (idempotent per day).

    Parameters:
        data (pd.DataFrame or str): A pandas DataFrame or path to a CSV file.
            Must already contain ``snapshot_column``.
        table_id (str): The ID of the history table in BigQuery.
        snapshot_date (str): The snapshot date (e.g. ``"2026-06-20"``) whose
            rows should be replaced.
        snapshot_column (str): The column holding the snapshot date.
        dataset_id (str): The ID of the dataset in BigQuery.
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

    # If the table already exists, remove any rows for this snapshot date so a
    # re-run replaces them instead of accumulating duplicates. On the very first
    # run the table doesn't exist yet, so there is nothing to delete.
    try:
        client.get_table(table_ref)
        delete_query = (
            f"DELETE FROM `{project_id}.{dataset_id}.{table_id}` "
            f"WHERE {snapshot_column} = @snapshot_date"
        )
        delete_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_date", "STRING", str(snapshot_date))
            ]
        )
        client.query(delete_query, job_config=delete_config).result()
    except NotFound:
        pass

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=True,
    )

    start_time = time.time()
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Waits for the job to complete
    elapsed_time = time.time() - start_time

    print(f"Appended {len(df)} snapshot rows for {snapshot_date} to "
          f"{dataset_id}.{table_id} in {elapsed_time:.2f} seconds")
