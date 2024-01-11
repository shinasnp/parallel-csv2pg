mport psycopg2
import concurrent.futures
import csv
import io
import multiprocessing
import threading

# Database connection parameters
db_params = {
    "host": "localhost",
    "port": "5432",
    "database": "data",
    "user": "root",
    "password": "1234",
}

# CSV file path
csv_file_path = "updated_csv_5gb.csv"


# Table name in the database
table_name = "customer"

# Number of processes to use
num_processes = 2

# Number of threads per process
num_threads_per_process = 5
total_rows = sum(1 for _ in open(csv_file_path))  # Chunk size for data processing

process_size = total_rows // num_processes


# Function to process a batch of rows and insert into the database
def process_and_insert_batch(chunk):
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        copy_sql = f"COPY {table_name} FROM stdin WITH CSV HEADER DELIMITER as ','"

        # Create a file-like object from the chunk
        csv_data = io.StringIO()
        csv_writer = csv.writer(csv_data)
        csv_writer.writerows(chunk)
        csv_data.seek(0)  # Move the cursor to the beginning of the file

        cursor.copy_expert(sql=copy_sql, file=csv_data)
        conn.commit()

        cursor.close()
        conn.close()
        print(f"Inserted {len(chunk)} rows.")

    except Exception as e:
        print(f"Error writing data to the '{table_name}' table: {str(e)}")


# Function to read and process the CSV file in chunks
def read_and_process_csv(start, end):
    chunk_size = 10000
    with open(csv_file_path, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header
        count = 0

        chunk = []
        for row in csv_reader:
            count += 1
            if count > end:
                break
            if not start < count < end:
                continue
            if not row[0] or not row[4] and row[5] < 550:
                continue

            chunk.append(row)

            if len(chunk) == chunk_size:
                process_and_insert_batch(chunk)
                chunk = []
                import gc

                gc.collect()

        if chunk:
            process_and_insert_batch(chunk)


# Function to run multithreading within each multiprocessing process
def run_multithreading(start, end):
    threading_size = (end - start + 1) // num_threads_per_process
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=num_threads_per_process
    ) as executor:
        futures = []

        for i in range(num_threads_per_process):
            thread_start = start + i * threading_size
            thread_end = start + (i + 1) * threading_size - 1

            if i == num_threads_per_process - 1:
                # Adjust the end for the last thread to ensure it covers the remaining rows
                thread_end = end

            # Submit the task to the thread pool
            future = executor.submit(read_and_process_csv, thread_start, thread_end)
            futures.append(future)

        # Wait for all tasks to complete
        concurrent.futures.wait(futures)


def main():
    processes = []
    for i in range(num_processes):
        start = i * process_size
        end = (i + 1) * process_size
        process = multiprocessing.Process(target=run_multithreading, args=(start, end))
        processes.append(process)
        process.start()

    # Wait for all processes to finish
    for process in processes:
        process.join()


if __name__ == "__main__":
    import time

    start_time = time.time()  # Record the start time

    main()  # Run the main processing

    end_time = time.time()  # Record the end time
    elapsed_time = end_time - start_time  # Calculate the elapsed time

    print(f"Total processing time: {elapsed_time:.2f} seconds")
