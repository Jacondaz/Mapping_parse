import ast
import json
import os.path
import re

import pandas as pd


def load_python_like_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

        content = re.sub(r'//.*$', '', content, flags=re.MULTILINE)
        content = re.sub(r'\bTrue\b', 'true', content)
        content = re.sub(r'\bFalse\b', 'false', content)
        content = re.sub(r'\bNone\b', 'null', content)

        content = re.sub(
            r'(?<=:)\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(?=[,\}\]])',
            lambda m: f' "{m.group(1)}"',
            content
        )

        try:
            return json.loads(content)
        except json.JSONDecodeError:
            try:
                return ast.literal_eval(content)
            except (SyntaxError, ValueError) as e:
                print(f"Ошибка при парсинге файла: {e}")
                return None


def compare_df_and_json(df, json_list):
    def make_key(item):
        return (
            str(item.get('name', item.get('Тэг в JSON', ''))).strip().lower().split(".")[-1],
            str(item.get('colType', item.get('Тип данных.1', ''))).strip().lower().split(".")[-1],
            str(item.get('alias', item.get('Код атрибута', ''))).strip().lower().split(".")[-1]
        )

    df_records = df.to_dict('records')
    df_keys = {make_key(item) for item in df_records}
    json_keys = {make_key(item) for item in json_list}

    df_not_in_json = [item for item in df_records if make_key(item) not in json_keys]
    json_not_in_df = [item for item in json_list if make_key(item) not in df_keys]

    return {
        'Элементы маппинга, которых нет в JSON': df_not_in_json,
        'Элементы JSON, которых нет в маппинге': json_not_in_df
    }


def create_df_json():
    results = dict()
    if url_mapping and any(os.path.isfile(os.path.join("dag_jsons", item)) for item in os.listdir("dag_jsons")):
        try:
            df_mapping = pd.read_excel(url_mapping, engine='openpyxl', sheet_name='Mapping', skiprows=1)
        except Exception as e:
            print(f"Error - {e}")
            return results

        tables = df_mapping['Таблица'].unique()
        json_dict = dict()

        for filename in os.listdir("dag_jsons"):
            if not filename.endswith('.json'):
                continue
            file_path = os.path.join("dag_jsons", filename)
            try:
                data = load_python_like_json(file_path)
                if data:
                    table_name = data["flows"][0]["target"]["table"]
                    json_dict[table_name] = data["flows"][0]["source"]["parsedColumns"]
            except Exception as e:
                print(f"Error processing {filename}: {e}")

        for table in tables:
            temp_json = json_dict.get(table, [])
            temp_df = df_mapping[df_mapping['Таблица'] == table].fillna("")

            if not temp_df.empty:
                temp_df = temp_df[['Тэг в JSON', 'Тип данных.1', 'Код атрибута']]
                results[table] = compare_df_and_json(temp_df, temp_json)
            else:
                results[table] = {'Отсутствует JSON для таблицы': table}

        with open('results.txt', 'w', encoding='utf-8') as f:
            for table, result in results.items():
                f.write(f"\n=== Результаты для таблицы: {table} ===\n")

                if 'Отсутствует JSON для таблицы' in result:
                    f.write(f"ВНИМАНИЕ: {result['Отсутствует JSON для таблицы']}\n")
                    continue

                if result['Элементы маппинга, которых нет в JSON']:
                    f.write("\nЭлементы маппинга, отсутствующие в JSON:\n")
                    for item in result['Элементы маппинга, которых нет в JSON']:
                        f.write(f" - {item}\n")
                else:
                    f.write("\nВсе элементы маппинга присутствуют в JSON\n")

                if result['Элементы JSON, которых нет в маппинге']:
                    f.write("\nЭлементы JSON, отсутствующие в маппинге:\n")
                    for item in result['Элементы JSON, которых нет в маппинге']:
                        f.write(f" - {item}\n")
                else:
                    f.write("\nВсе элементы JSON присутствуют в маппинге\n")

        print("Результаты сравнения сохранены в файл results.txt")
    return results


if __name__ == '__main__':
    url_mapping = None
    create_df_json()