# Parallel CSV to PostgreSQL Uploader

Efficiently upload large CSV data (5GB) to PostgreSQL using concurrent processing and multithreading in Python.

## Usage

1. Update the `db_params` dictionary with your PostgreSQL database connection details.
2. Set the `csv_file_path` variable to the path of your CSV file.
3. Adjust the `num_processes` and `num_threads_per_process` variables to optimize performance.
4. Run the script to process and upload the CSV data.

## Configuration

- **Database Connection:** Update `db_params` with your PostgreSQL database details.
- **CSV File:** Set `csv_file_path` to the path of the CSV file to be uploaded.
- **Table Name:** Specify the target table in the database using `table_name`.
- **Processing Configuration:** Tune `num_processes` and `num_threads_per_process` for optimal performance.

## Execution

1. **CSV to PostgreSQL Uploader:**

   ```bash
   python parallel_csv2pg.py
2. **Data generation:**

   ```bash
   python generate_data.py.py


