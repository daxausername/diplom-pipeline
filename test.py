import os
import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi
os.environ['KAGGLE_CONFIG_DIR'] = '.kaggle'

dataset = "ealaxi/paysim1"
filename = 'PS_20174392719_1491204439457_log.csv'

def get_dataset_kaggle(dataset, filename):
    if os.path.exists(filename):
        print(f"{filename} уже есть, пропускаем скачивание")
        return
    
    os.environ['KAGGLE_CONFIG_DIR'] = '.kaggle'

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset=dataset, path='.', unzip=True)
    print(f"Файл успешно распакован {dataset}")

get_dataset_kaggle(dataset, filename)