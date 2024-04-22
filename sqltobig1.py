import tkinter as tk
from tkinter import filedialog
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
    console_output.insert(tk.END, message + "\n")
    console_output.see(tk.END)  # Scroll to the end of the console


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
                print("Transfer stopped by user.")
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
                        print("Transfer stopped by user.")
                        break
                    csv_writer.writerow(row)
                    total_rows_transferred += 1

            append_to_console(
                f"Data from table '{table_name}' has been uploaded to GCS bucket: gs://{bucket_name}/{blob_name}"
            )
        # Transfer data from GCS to BigQuery
        transfer_to_bigquery()

    except Exception as e:
        print(f"An error occurred: {str(e)}")

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

    PROJECT_ID = project_entry.get()
    DATASET_NAME = dataset_entry.get()
    BUCKET_NAME = bucket_entry.get()

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
        not server_entry.get()
        or not database_entry.get()
        or not username_entry.get()
        or not password_entry.get()
        or not json_file_entry.get()
        or not bucket_entry.get()
        or not dataset_entry.get()
        or not project_entry.get()
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
        "server": server_entry.get(),
        "database": database_entry.get(),
        "username": username_entry.get(),
        "password": password_entry.get(),
    }
    bucket_name = bucket_entry.get()
    dataset_name = dataset_entry.get()

    # Export data from SQL Server to GCS
    export_to_gcs(sql_server_details, bucket_name)

    # Save UI data
    save_ui_data(sql_server_details, JSON_AUTH_FILE_PATH, bucket_name, dataset_name)


def select_json_file():
    global JSON_AUTH_FILE_PATH
    JSON_AUTH_FILE_PATH = filedialog.askopenfilename(
        title="Select JSON file",
        filetypes=(("JSON files", "*.json"), ("All files", "*.*")),
    )
    json_file_entry.delete(0, tk.END)
    json_file_entry.insert(0, JSON_AUTH_FILE_PATH)


def stop_button_click():
    global stop_transfer
    stop_transfer = True


# Define save_ui_data function to accept UI elements as arguments
import os


def save_ui_data(sql_server_details, json_file_path, bucket_name, dataset_name):
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
        file.write(f"Bucket Name: {bucket_name}\n")
        file.write(f"Dataset Name: {dataset_name}\n")
        file.write(f"Project ID: {project_entry}\n")

    append_to_console("Server data saved successfully.")


# Create main window
root = tk.Tk()
root.title("Export SQL Server Data to GCS and Transfer to BigQuery")
root.geometry("800x450")
root.configure(bg="#2C3E50")
root.resizable(False, False)

for i in range(10):
    root.rowconfigure(i, weight=1)
for j in range(3):
    root.columnconfigure(j, weight=1)

# SQL Server details
server_label = tk.Label(root, text="Server:", bg="#2C3E50", fg="white")
server_label.grid(row=0, column=0, sticky=tk.E)
server_entry = tk.Entry(root)
server_entry.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)

database_label = tk.Label(root, text="Database:", bg="#2C3E50", fg="white")
database_label.grid(row=0, column=1, sticky=tk.E)
database_entry = tk.Entry(root)
database_entry.grid(row=0, column=2, padx=5, pady=5, sticky=tk.W)

username_label = tk.Label(root, text="Username:", bg="#2C3E50", fg="white")
username_label.grid(row=1, column=0, sticky=tk.E)
username_entry = tk.Entry(root)
username_entry.grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)

password_label = tk.Label(root, text="Password:", bg="#2C3E50", fg="white")
password_label.grid(row=1, column=1, sticky=tk.E)
password_entry = tk.Entry(root, show="*")
password_entry.grid(row=1, column=2, padx=5, pady=5, sticky=tk.W)

# BigQuery Dataset Name
dataset_label = tk.Label(root, text="Dataset Name:", bg="#2C3E50", fg="white")
dataset_label.grid(row=2, column=0, sticky=tk.E)
dataset_entry = tk.Entry(root)
dataset_entry.grid(row=2, column=1, padx=5, pady=5, sticky=tk.W)

# JSON file path
json_file_label = tk.Label(root, text="JSON File Path:", bg="#2C3E50", fg="white")
json_file_label.grid(row=2, column=1, sticky=tk.E)
json_file_entry = tk.Entry(root)
json_file_entry.grid(row=2, column=2, padx=5, pady=5, sticky=tk.W, columnspan=2)

# Load your icon image (replace 'path_to_icon.png' with the actual path to your icon file)
# Load your icon image
icon_image = tk.PhotoImage(file=r"C:\Users\admin\Downloads\search.png")

# Resize the icon by a factor of 2
icon_image_resized = icon_image.subsample(17, 16)

# Set the background color of the button to match the window background
json_file_button = tk.Button(
    root,
    command=select_json_file,
    image=icon_image_resized,
    bg="#2C3E50",
    highlightthickness=0,
    bd=0,
    activebackground="#2C3E50",
)
json_file_button.place(x=650, y=90)

# GCS Bucket Name
bucket_label = tk.Label(root, text="Bucket Name:", bg="#2C3E50", fg="white")
bucket_label.grid(row=3, column=0, sticky=tk.E)
bucket_entry = tk.Entry(root)
bucket_entry.grid(row=3, column=1, padx=5, pady=5, sticky=tk.W)

project_label = tk.Label(root, text="Project ID:", bg="#2C3E50", fg="white")
project_label.grid(row=3, column=1, sticky=tk.E)
project_entry = tk.Entry(root)
project_entry.grid(row=3, column=2, padx=5, pady=5, sticky=tk.W)

# Export and Transfer Button
export_transfer_button = tk.Button(
    root,
    text="Migrate",
    command=export_and_transfer_button_click,
    bg="#3498DB",
    fg="white",
)
export_transfer_button.place(x=350, y=180)

# Stop Button
stop_button = tk.Button(
    root, text="Cancel", command=stop_button_click, bg="#E74C3C", fg="white"
)
stop_button.place(x=450, y=180)

# Console Output
console_frame = tk.Frame(root, bg="#34495E")
console_frame.grid(row=6, column=0, columnspan=4, sticky="nsew", padx=10, pady=5)

console_label = tk.Label(console_frame, text="Console Output", bg="#34495E", fg="white")
console_label.pack(pady=(0, 3))

console_output = tk.Text(
    console_frame, wrap=tk.WORD, bg="black", fg="white", height=10, width=80
)
console_output.pack(expand=True, fill=tk.BOTH)

# Start the GUI event loop
root.mainloop()
