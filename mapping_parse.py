import os

import pandas as pd
import json

def parse():

    with open('source/template.json', 'r', encoding='utf-8') as file:
        data = json.load(file)

    try:
        df_mapping = pd.read_excel(mapping_url, engine='openpyxl', sheet_name='Mapping', skiprows=1)
        # if lmd_url:
        #     with open('source/template.json', 'r', encoding='utf-8') as file_lmd:
        #         data_lmd = json.load(file_lmd)
    except Exception as e:
        print(f"Error - {e}")
    else:
        tables = df_mapping['Таблица'].unique()
        os.makedirs("output", exist_ok=True)

        for table in tables:
            temp_df = df_mapping[df_mapping['Таблица'] == table].fillna("")
            temp_list = list()

            for _, row in temp_df.iterrows():
                if "hdp_processed_dttm" in row['Код атрибута']:
                    continue
                else:
                    temp_list.append({"name": row['Тэг в JSON'],"colType": row['Тип данных.1'],"alias": row['Код атрибута']})

            parsed_columns_str = "[\n" + " " * 20 + (",\n" + " " * 20).join(
                [json.dumps(col, ensure_ascii=False, separators=(", ", ": ")) for col in temp_list]
            ) + "\n" + " " * 16 + "]"

            json_str = json.dumps(data, indent=4, ensure_ascii=False)

            json_str = json_str.replace('"parsedColumns": []', f'"parsedColumns": {parsed_columns_str}')

            with open(f"output/{table}.json", "w", encoding="utf-8") as file:
                file.write(json_str)


if __name__ == '__main__':
    mapping_url = None  # Сюда путь на маппинг
    lmd_url = None  # Сюда путь на лмд
    parse()