import streamlit as st
import pyodbc
import csv
import os
from google.cloud import storage
from google.cloud import bigquery
import datetime  # Import datetime module for date and time operations

# Global variables
JSON_AUTH_FILE_PATH = ""
stop_transfer = False


# Define the append_to_console function to add messages to the console output
def append_to_console(message):
    st.text(message)


# Define the get_current_datetime function to get the current date and time
def get_current_datetime():
    now = datetime.datetime.now()
    return now.strftime("%Y-%m-%d_%H-%M-%S")


# Define export_to_gcs to accept UI elements as arguments
def export_to_gcs(sql_server_details, bucket_name):
    global stop_transfer
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={sql_server_details['server']};"
            f"DATABASE={sql_server_details['database']};"
            f"UID={sql_server_details['username']};"
            f"PWD={sql_server_details['password']};"
        )

        # Create a cursor from the connection
        cursor = conn.cursor()

        # Query to get all tables in the database
        query_tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        cursor.execute(query_tables)
        tables = cursor.fetchall()

        total_rows_transferred = 0  # Counter for total rows transferred

        # Initialize the Google Cloud Storage client
        storage_client = storage.Client()

        # Iterate over each table
        for table in tables:
            if stop_transfer:
                append_to_console("Transfer stopped by user.")
                break

            table_name = table[0]
            append_to_console(f"Exporting data from table: {table_name}")

            # Query to select all data from the current table
            query_data = f"SELECT * FROM {table_name}"
            cursor.execute(query_data)

            # Specify the name of the object in the bucket
            blob_name = f"{table_name}.csv"

            # Get the bucket
            bucket = storage_client.bucket(bucket_name)

            # Create a blob object in the bucket
            blob = bucket.blob(blob_name)

            # Write data directly to the blob
            with blob.open("w", encoding="utf-8", newline="") as file:
                csv_writer = csv.writer(file)

                # Write the header
                header = [column[0] for column in cursor.description]
                csv_writer.writerow(header)

                # Write data rows and track progress
                for row in cursor.fetchall():
                    if stop_transfer:
                        append_to_console("Transfer stopped by user.")
                        break
                    csv_writer.writerow(row)
                    total_rows_transferred += 1

            append_to_console(
                f"Data from table '{table_name}' has been uploaded to GCS bucket: gs://{bucket_name}/{blob_name}"
            )
        # Transfer data from GCS to BigQuery
        transfer_to_bigquery()

    except Exception as e:
        append_to_console(f"An error occurred: {str(e)}")

    finally:
        # Close the cursor and connection
        if "cursor" in locals() and cursor:
            cursor.close()
        if "conn" in locals() and conn:
            conn.close()


# Define transfer_to_bigquery to accept UI elements as arguments
def transfer_to_bigquery():
    global JSON_AUTH_FILE_PATH

    if not JSON_AUTH_FILE_PATH:
        append_to_console("Please select the JSON authentication file.")
        return

    PROJECT_ID = st.session_state.project_id
    DATASET_NAME = st.session_state.dataset_name
    BUCKET_NAME = st.session_state.bucket_name

    bigquery_client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)

    bucket = storage_client.get_bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()

    for blob in blobs:
        if blob.name.endswith(".csv"):
            table_name = os.path.splitext(os.path.basename(blob.name))[0]
            dataset_ref = bigquery_client.dataset(DATASET_NAME)
            table_ref = dataset_ref.table(table_name)

            try:
                bigquery_client.get_table(table_ref)
                append_to_console(
                    f"Table {table_name} already exists. Skipping load for {blob.name}"
                )
            except Exception as e:
                try:
                    job_config = bigquery.LoadJobConfig(
                        autodetect=True,
                        skip_leading_rows=1,
                        source_format=bigquery.SourceFormat.CSV,
                        max_bad_records=1000,  # Adjust this value as needed
                    )
                    uri = f"gs://{BUCKET_NAME}/{blob.name}"
                    load_job = bigquery_client.load_table_from_uri(
                        uri, table_ref, job_config=job_config
                    )
                    load_job.result()  # Waits for the job to complete.
                    append_to_console(
                        f"Data from {blob.name} has been loaded into {table_name}"
                    )
                except Exception as load_error:
                    append_to_console(
                        f"Error loading data from {blob.name}: {load_error}"
                    )


def validate_ui_fields():
    # Check if all required fields are filled
    if (
        not st.session_state.server
        or not st.session_state.database
        or not st.session_state.username
        or not st.session_state.password
        or not JSON_AUTH_FILE_PATH
        or not st.session_state.bucket_name
        or not st.session_state.dataset_name
        or not st.session_state.project_id
    ):
        return False
    return True


# Modify export_and_transfer_button_click function to accept UI elements as arguments
def export_and_transfer_button_click():
    global stop_transfer
    stop_transfer = False
    if not validate_ui_fields():
        append_to_console("Please fill in all fields before initiating the migration.")
        return

    sql_server_details = {
        "server": st.session_state.server,
        "database": st.session_state.database,
        "username": st.session_state.username,
        "password": st.session_state.password,
    }
    bucket_name = st.session_state.bucket_name

    # Export data from SQL Server to GCS
    export_to_gcs(sql_server_details, bucket_name)

    # Save UI data
    save_ui_data(sql_server_details, JSON_AUTH_FILE_PATH)


def select_json_file(uploaded_file):
    global JSON_AUTH_FILE_PATH
    JSON_AUTH_FILE_PATH = uploaded_file
    st.session_state.json_file_path = JSON_AUTH_FILE_PATH


def stop_button_click():
    global stop_transfer
    stop_transfer = True


# Define save_ui_data function to accept UI elements as arguments
def save_ui_data(sql_server_details, json_file_path):
    # Create a folder if it doesn't exist
    folder_name = "UI_Data"
    os.makedirs(folder_name, exist_ok=True)

    # Get the current username
    username = sql_server_details["username"]

    # Get the current date and time
    current_datetime = get_current_datetime()

    # Construct the file name
    file_name = f"{username}_{current_datetime}.txt"

    # Construct the file path
    file_path = os.path.join(folder_name, file_name)

    # Write UI data to a text file
    with open(file_path, "w") as file:
        file.write("SQL Server Details:\n")
        file.write(f"Server: {sql_server_details['server']}\n")
        file.write(f"Database: {sql_server_details['database']}\n")
        file.write(f"Username: {sql_server_details['username']}\n")
        file.write(f"Password: {sql_server_details['password']}\n")
        file.write(f"JSON File Path: {json_file_path}\n")
        file.write(f"Bucket Name: {st.session_state.bucket_name}\n")
        file.write(f"Dataset Name: {st.session_state.dataset_name}\n")
        file.write(f"Project ID: {st.session_state.project_id}\n")

    append_to_console("Server data saved successfully.")


# Streamlit interface

st.title("Export SQL Server Data to GCS and Transfer to BigQuery")

# SQL Server details
st.session_state.server = st.text_input("Server")
st.session_state.database = st.text_input("Database")
st.session_state.username = st.text_input("Username")
st.session_state.password = st.text_input("Password", type="password")

# BigQuery Dataset Name
st.session_state.dataset_name = st.text_input("Dataset Name")

# JSON file path
uploaded_file = st.file_uploader("Select JSON file", type=["json"])
if uploaded_file:
    select_json_file(uploaded_file)

# GCS Bucket Name
st.session_state.bucket_name = st.text_input("Bucket Name")

# Project ID
st.session_state.project_id = st.text_input("Project ID")

# Export and Transfer Button
if st.button("Migrate"):
    export_and_transfer_button_click()

# Stop Button
if st.button("Cancel"):
    stop_button_click()

# Console Output
st.text_area("Console Output", height=10, disabled=True)

