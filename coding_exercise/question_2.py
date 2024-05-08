# QUESTION 2:
import io
import json
from datetime import datetime

import pandas as pd


# Function to convert epoch time to readable timestamp
def convert_epoch_to_timestamp(epoch):
    return datetime.fromtimestamp(int(epoch) / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

# Read the data
data = """topic|offset|data|epoctime
kafkatopic1|1|{"metadata": {"Id": 1, "balance": 1,"result":3},"balances": {"Id": 1, "name": "C1", "status": "S", "busDate": "2023-11-10"}, "testResult": [{"resultId": 1, "Id": 1, "resultName": "Maths", "resultValue": 98.0}, {"resultId": 2, "Id": 1, "resultName": "English", "resultValue": 98.0}, {"resultId": 3, "Id": 1, "resultName": "Science", "resultValue": 95.0}]}|1700736290127
kafkatopic1|2|{"metadata": {"Id": 2, "balance": 1,"result":3}, "balances": {"Id": 2, "name": "C2", "status": "S", "busDate": "2023-11-10"}, "testResult": [{"resultId": 1, "Id": 2, "resultName": "Maths", "resultValue": 93.0}, {"resultId": 2, "Id": 2, "resultName": "English", "resultValue": 98.0}, {"resultId": 3, "Id": 2, "resultName": "Science", "resultValue": 94.0}]}|1700736290128
kafkatopic1|3|{"metadata": {"Id": 3, "balance": 1,"result":3}, "balances": {"Id": 3, "name": "C3", "status": "S", "busDate": "2023-11-10"}, "testResult": [{"resultId": 1, "Id": 3, "resultName": "Maths", "resultValue": 93.0}, {"resultId": 2, "Id": 3, "resultName": "English", "resultValue": 98.0}, {"resultId": 3, "Id": 3, "resultName": "Science", "resultValue": 94.0}]}|1700736290129"""
df = pd.read_csv(io.StringIO(data), delimiter='|')

# Prepare dataframes for metadata and results
metadata_list = []
results_list = []

for index, row in df.iterrows():
    json_data = json.loads(row['data'])
    metadata = json_data['metadata']
    balances = json_data['balances']
    test_result = json_data['testResult']

    bus_date = convert_epoch_to_timestamp(row['epoctime'])

    # Metadata processing
    metadata_dict = {
        'Id': metadata['Id'],
        'balance': metadata['balance'],
        'result': metadata['result'],
        'offset': row['offset'],
        'busDate': bus_date
    }
    metadata_list.append(metadata_dict)

    # Results processing
    if test_result:
        for result in test_result:
            result_dict = {
                'resultId': result['resultId'],
                'Id': result['Id'],
                'resultName': result['resultName'],
                'resultValue': result['resultValue'],
                'busDate': bus_date
            }
            results_list.append(result_dict)
    else:
        # Handling scenario where testResult is null or empty
        results_list.append({
            'resultId': None,
            'Id': None,
            'resultName': None,
            'resultValue': None,
            'busDate': bus_date
        })

# Creating DataFrames
metadata_df = pd.DataFrame(metadata_list)
results_df = pd.DataFrame(results_list)

# Write to CSV
timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
metadata_df.to_csv(f'metadata_{timestamp}.csv', index=False)
results_df.to_csv(f'result_{timestamp}.csv', index=False)

print("Files generated successfully.")
