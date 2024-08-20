import subprocess
import os
import csv
import sys

openmldb_binary_path = sys.argv[1]
# Execute the offline SQL command
subprocess.run(f"{openmldb_binary_path} --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < offline.sql", 
               shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

# Define the directory containing the CSV files
csv_dir = "/tmp/offline.csv/"

# List all files in the directory
files = os.listdir(csv_dir)

# Filter out non-CSV files (and avoid reading .crc files)
csv_files = [file for file in files if file.endswith('.csv')]

# Initialize an empty list to store the combined data
combined_data = []

# Read and concatenate all CSV files
for file in csv_files:
    with open(os.path.join(csv_dir, file), newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            combined_data.append(row)

# Define the command to be executed
command = f"{openmldb_binary_path} --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < online.sql"

# Execute the command
try:
    result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output = result.stdout

    # Extract the relevant line containing the data
    lines = output.splitlines()
    for i, line in enumerate(lines):
        if "c1" in line and "c2" in line and "w1_c3_sum" in line:
            data_line = lines[i + 2]  # The line containing the data is two lines below the header
            break

    # Split the line into an array
    data_array = data_line.split()

    # Check if the specific row exists
    row_exists = any(row == data_array for row in combined_data)
    
    if row_exists:
        print("Online and offline data consistent")
    else:
        print("Online and offline data not consistent")

    # Print the resulting array
except subprocess.CalledProcessError as e:
    print("An error occurred while executing the command:", e)
    print("Error Output:\n", e.stderr)
