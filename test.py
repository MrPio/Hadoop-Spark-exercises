import csv
import os

csv_files = []
for root, dirs, files in os.walk('dataset'):
    csv_files += [os.path.join(root, file) for file in files if file.endswith('.csv')]
for csv_file in csv_files:
    with open(csv_file, 'r', newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        header = next(csv_reader)
        print(header)
        print(csv_file, header.index('LATITUDE'), header.index('TMP'), header.index('WND'))


