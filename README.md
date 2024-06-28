

# SQL Server to GCP BigQuery Data Migration

This Python script facilitates the migration of data from a Microsoft SQL Server database to Google Cloud Platform's BigQuery. It exports data to Google Cloud Storage (GCS) first and then transfers it to BigQuery. Below is an overview of its functionality and setup.

## Overview

The script provides a graphical user interface (GUI) using tkinter for:

- Setting SQL Server connection details.
- Specifying GCS bucket, BigQuery dataset, and GCP project ID.
- Initiating and monitoring the data export and transfer processes.
- Logging progress and messages to the console.

## Features

### Libraries Used

- **tkinter**: GUI toolkit for Python.
- **pyodbc**: SQL Server connection library.
- **csv**: Handling CSV files.
- **os**: Operating system operations.
- **Google Cloud Libraries**: Interacting with GCS and BigQuery.

### Functions

- **append_to_console**: Appends messages to the GUI console output.
- **get_current_datetime**: Retrieves the current date and time.
- **export_to_gcs**: Exports SQL Server data to GCS.
- **transfer_to_bigquery**: Transfers data from GCS to BigQuery.
- **validate_ui_fields**: Validates user interface fields.
- **export_and_transfer_button_click**: Initiates export and transfer processes.
- **select_json_file**: Allows selection of a JSON authentication file.
- **stop_button_click**: Stops the data transfer process.
- **save_ui_data**: Saves user interface data to a text file.

## Usage

1. **Setup**:
   - Install necessary Python libraries (`pyodbc`, `google-cloud-storage`, `google-cloud-bigquery`).
   - Ensure you have a Google Cloud project with BigQuery and Cloud Storage enabled.

2. **Configuration**:
   - Modify `JSON_AUTH_FILE_PATH` and other global variables as needed.
   - Set SQL Server connection details and GCP credentials in the GUI.

3. **Execution**:
   - Run the script (`sqltobig1.py`) to open the GUI.
   - Enter details in the GUI fields.
   - Click the "Export and Transfer" button to start the migration process.

4. **Console Output**:
   - Monitor progress and messages in the GUI console.
   - Use the "Stop" button to halt the process if necessary.

## Additional Notes

- This project assumes familiarity with Python, SQL Server, Google Cloud Platform, and BigQuery.
- Ensure proper authentication and authorization for GCP services.
- For issues or improvements, please submit a GitHub issue or pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Feel free to adjust the sections and details based on additional features or specific configurations relevant to your implementation. This README provides a clear overview of the project's purpose, setup instructions, features, and usage guidelines.
