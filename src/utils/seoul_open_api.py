import requests
import json
import pandas as pd


def create_api_url(key: str, service_id: str, *args) -> str:
    url = f"http://openapi.seoul.go.kr:8088/{key}/json/{service_id}"
    for arg in args:
        url += f"/{arg}"
    return url


def create_url(
    key, service_id, start_index, end_index, year_code=None, format="json"
) -> str:
    url = f"http://openapi.seoul.go.kr:8088/{key}/{format}/{service_id}/{start_index}/{end_index}"
    if year_code is not None:
        url += f"/{year_code}"

    return url


def get_result_json(url, service_id):
    response = requests.get(url)
    result = response.content.decode()
    result_json = json.loads(result)
    return result_json[service_id]


def get_list_total_count(key, service_id, year_code=None) -> str:
    test_url = create_url(key, service_id, 1, 1, year_code)
    result_json = get_result_json(test_url, service_id)
    data = result_json["list_total_count"]

    return data


def get_data_list(key, service_id, start_index, end_index, year_code=None):
    url = create_url(key, service_id, start_index, end_index, year_code)
    data_list = get_result_json(url, service_id)
    row = data_list["row"]
    return row


def get_all_data_list(key, service_id, batch_size=999):
    df = pd.DataFrame()
    list_total_count = get_list_total_count(key, service_id)
    for start_index in range(1, list_total_count, batch_size):
        end_index = min(start_index + batch_size, list_total_count)
        data_list = get_data_list(key, service_id, start_index, end_index)

        list_df = pd.DataFrame(data_list)
        df = pd.concat([df, list_df])
    return df


def get_data_list_by_year(key, service_id, year_code, batch_size=999):
    df = pd.DataFrame()
    list_total_count = get_list_total_count(key, service_id, year_code)
    for start_index in range(1, list_total_count, batch_size):
        end_index = min(start_index + batch_size, list_total_count)
        data_list = get_data_list(key, service_id, start_index, end_index, year_code)

        list_df = pd.DataFrame(data_list)
        df = pd.concat([df, list_df])

    return df
