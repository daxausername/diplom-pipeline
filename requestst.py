import requests
import os, json, subprocess, csv
import pandas as pd

def get_dataset_kaggle(dataset, filename):
    # тут нужен оброботчик ибо если файл уже есть нахрен его качать 
    if os.path.exists(filename):
        return from_csv_to_json()
    os.environ['KAGGLE_CONFIG_DIR'] = '.kaggle'
    import kaggle

    kaggle.api.dataset_download_files(dataset = dataset, unzip=True)
    return from_csv_to_json()

def from_csv_to_json():
    
    with open(filename, encoding = 'ISO-8859-1') as csv_file_fraud:
        # а как мне обработать именно 1000000 строк в csv файле а потом сместиться по строчкам и считать следующие 100000 в рамках исполнения dag(a)
        csv_reader = csv.DictReader(csv_file_fraud)
        data = []
        for i, row in enumerate(csv_reader):
            if i == 10:
                break
            data.append(row)


        
    json_output = json.dumps(data, indent = 4)
    return json_output

# def format_data(json_output):
#     data = {}
#     for entry in json_output:
#         # может добавить uuid?
#         data['step'] = json_output['step']
#         data['transaction_type'] = json_output['type']
#         data['amount'] = json_output['amount']
#         data['sender_id'] = json_output['nameOrig']
#         data['initial_balance_sender'] = json_output['oldbalanceOrg']
#         data['finite_balance_sender'] = json_output['newbalanceOrig']
#         data['recipient_id'] = json_output['nameDest']
#         data['initial_balance_recipient'] = json_output['oldbalanceDest']
#         data['finite_balance_recipient'] = json_output['newbalanceDest']
#         data['is_fraud'] = json_output['isFraud']
#         data['is_flagged_fraud'] = json_output['isFlaggedFraud']
#     return data

dataset = "ealaxi/paysim1"
filename = 'PS_20174392719_1491204439457_log.csv'


json_batch = get_dataset_kaggle(dataset, filename)

print(json_batch)

# data = format_data(json_batch)


# print(data)



