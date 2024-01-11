import csv
import random
import string

# Function to generate a random name
def random_name():
    return ''.join(random.choice(string.ascii_letters) for _ in range(10))

# Function to generate a random email
def random_email():
    username_length = random.randint(5, 10)
    domain_length = random.randint(5, 10)

    username = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(username_length))
    domain = ''.join(random.choice(string.ascii_letters + string.digits + '.' + '-') for _ in range(domain_length))

    return f"{username}@{domain}.com"

# Function to generate a random phone number
def random_phone():
    return ''.join(random.choice(string.digits) for _ in range(10))


# Function to generate random data for each row
def generate_random_data():
    cust_id = random.randint(1, 1000000)
    name = random_name()
    gender = random.choice(["male", "female"])
    email = random_email()
    credit_score = random.randint(300, 850)

    return [cust_id, name, gender, email,credit_score]
# Specify the output file name
output_file = 'updated_csv_5gb.csv'
# Specify the desired file size in bytes (5 GB)
desired_file_size = 5000 * 1024 * 1024

import time 
start_time = time.time()

# Generate rows until the desired file size is reached
with open(output_file, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(["cust_id", "name", "gender", "email", "credit_score"])
    
    while True:
        row = generate_random_data()
        csv_writer.writerow(row)
        # Check if the file size has reached the desired size
        if csvfile.tell() >= desired_file_size:
            break

# Measure the end time
end_time = time.time()

# Calculate elapsed time
elapsed_time = end_time - start_time

# Print the time taken
print(f"Processing complete. Total time taken: {elapsed_time:.2f} seconds")
