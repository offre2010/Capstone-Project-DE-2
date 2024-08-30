from google.cloud import bigquery

def load_csv_to_bigquery(dataset_id, table_id, csv_file_path, service_account_key):
    # initialized client to bigquery
    client = bigquery.Client.from_service_account_json(service_account_key)

    ## Setting the URI
    uri = f"gs://{csv_file_path}"

    #configure loading job
    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.CSV,
        skip_leading_rows = 1,
        autodetect = True
    )

    ## Set Table
    table_ref = client.dataset(dataset_id).table(table_id)

    ## Load Job
    load_job = client.load_table_from_uri(
        uri, 
        table_ref,
        job_config=job_config
    )

    #Wait for the job completion
    load_job.result()

    #Verifying load job
    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows} rows into {table_id}")


def main():
    dataset_id = "caps_staging"
    service_account_key = "service_account_key.json"

    #looping through files
    files = [
        {"filepath": "caps-bucket/AGENCYMaster_clean.csv", "table_id": "agency"},
        {"filepath": "caps-bucket/EMPLOYEEMaster_clean.csv", "table_id": "employee"},
        {"filepath": "caps-bucket/NYCPayroll_2020_clean.csv", "table_id": "payroll_2020"},
        {"filepath": "caps-bucket/NYCPayroll_2021_clean.csv", "table_id": "payroll_2021"},
        {"filepath": "caps-bucket/TITLEMaster_clean.csv", "table_id": "title"}
    ]

    for file in files:
        load_csv_to_bigquery(dataset_id, file['table_id'], file['filepath'], service_account_key)



if __name__ == "__main__":
    main()