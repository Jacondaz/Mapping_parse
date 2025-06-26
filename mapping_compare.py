import os.path

import pandas as pd

def compare():
    if url_mapping and any(os.path.isfile(os.path.join("dag_jsons", item)) for item in os.listdir("dag_jsons")):
        try:
            df_mapping = pd.read_excel(url_mapping, engine='openpyxl', sheet_name='Mapping', skiprows=1)
        except Exception as e:
            print(f"Error - {e}")
        else:
            tables = df_mapping['Таблица'].unique()
            for table in tables:
                



if __name__ == '__main__':
    url_mapping = None #Сюда путь на маппинг
    compare()