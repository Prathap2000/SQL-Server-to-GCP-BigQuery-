# SQL-Server-to-GCP-BigQuery-
data migration from ms sql server to gcp bigquery 

 Python program designed to export data from a SQL Server database to Google Cloud Storage (GCS) and then transfer it to BigQuery. Here's a brief overview of what the script does:

1. **Importing Libraries**: The script imports necessary libraries including `tkinter` for GUI, `filedialog` for file selection, `pyodbc` for SQL Server connection, `csv` for CSV file operations, `os` for operating system operations, and Google Cloud libraries for interacting with GCS and BigQuery.

2. **Global Variables**: It defines some global variables including `JSON_AUTH_FILE_PATH` and `stop_transfer`.

3. **Functions**:
   - `append_to_console`: Appends messages to the console output.
   - `get_current_datetime`: Gets the current date and time.
   - `export_to_gcs`: Exports data from SQL Server to GCS.
   - `transfer_to_bigquery`: Transfers data from GCS to BigQuery.
   - `validate_ui_fields`: Validates the user interface fields.
   - `export_and_transfer_button_click`: Initiates the export and transfer process when the corresponding button is clicked.
   - `select_json_file`: Allows the user to select a JSON authentication file.
   - `stop_button_click`: Stops the data transfer process.
   - `save_ui_data`: Saves the user interface data to a text file.

4. **Main GUI Setup**:
   - It creates a main window using `tkinter`.
   - Sets up labels, entry fields, buttons, and console output for user interaction and feedback.

5. **Event Loop**: Starts the GUI event loop to handle user interactions.

This script provides a graphical interface for users to specify SQL Server details, GCS bucket, BigQuery dataset, and project ID. It then exports data from SQL Server to GCS and transfers it to BigQuery. The console output provides feedback on the progress of the export and transfer operations. Additionally, it saves UI data to a text file for record-keeping.
