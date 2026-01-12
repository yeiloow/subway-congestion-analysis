import pandas as pd
import os


def check_diff():
    file_path_2023 = "data/01_raw/서울교통공사_지하철혼잡도정보_20231231.csv"
    # file_path_2023 = "data/01_raw/서울교통공사_지하철혼잡도정보_20240331.csv"
    # file_path_2024 = "data/01_raw/서울교통공사_지하철혼잡도정보_20240630.csv"
    # file_path_2024 = "data/01_raw/서울교통공사_지하철혼잡도정보_20241231.csv"
    file_path_2024 = "data/01_raw/서울교통공사_지하철혼잡도정보_20250331.csv"

    # Check if files exist
    if not os.path.exists(file_path_2023):
        print(f"Error: {file_path_2023} not found.")
        return
    if not os.path.exists(file_path_2024):
        print(f"Error: {file_path_2024} not found.")
        return

    # Load data
    # Encoding might be 'euc-kr' or 'cp949' for Korean CSVs usually, but utf-8 is also possible.
    # The previous `head` command output showed Korean characters correctly, so it likely utf-8 or compatible.
    # Let's try reading with default encoding first, if fail then cp949.
    try:
        df_2023 = pd.read_csv(file_path_2023, encoding="euc-kr")
    except UnicodeDecodeError:
        df_2023 = pd.read_csv(file_path_2023, encoding="utf-8")

    try:
        df_2024 = pd.read_csv(file_path_2024, encoding="euc-kr")
    except UnicodeDecodeError:
        df_2024 = pd.read_csv(file_path_2024, encoding="utf-8")

    # Extract unique (station_number, station_name) pairs
    # Assuming columns are '역번호' and '출발역' based on previous inspection
    cols = ["역번호", "출발역"]

    pairs_2023 = df_2023[cols].drop_duplicates().sort_values(by="역번호")
    pairs_2024 = df_2024[cols].drop_duplicates().sort_values(by="역번호")

    set_2023 = set(tuple(x) for x in pairs_2023.to_numpy())
    set_2024 = set(tuple(x) for x in pairs_2024.to_numpy())

    only_in_2023 = sorted(list(set_2023 - set_2024), key=lambda x: x[0])
    only_in_2024 = sorted(list(set_2024 - set_2023), key=lambda x: x[0])

    print(f"Total unique stations in 2023: {len(set_2023)}")
    print(f"Total unique stations in 2024: {len(set_2024)}")

    print("\n--- Pairs in 2023 but not in 2024 ---")
    if only_in_2023:
        for num, name in only_in_2023:
            print(f"역번호: {num}, 출발역: {name}")
    else:
        print("None")

    print("\n--- Pairs in 2024 but not in 2023 ---")
    if only_in_2024:
        for num, name in only_in_2024:
            print(f"역번호: {num}, 출발역: {name}")
    else:
        print("None")

    # Check for same ID different Name
    dict_2023_id = {x[0]: x[1] for x in set_2023}
    dict_2024_id = {x[0]: x[1] for x in set_2024}

    common_ids = set(dict_2023_id.keys()) & set(dict_2024_id.keys())
    diff_names = []
    for sid in common_ids:
        if dict_2023_id[sid] != dict_2024_id[sid]:
            diff_names.append((sid, dict_2023_id[sid], dict_2024_id[sid]))

    if diff_names:
        print("\n--- Same ID but different Name (ID: 2023 -> 2024) ---")
        for sid, n1, n2 in sorted(diff_names):
            print(f"{sid}: {n1} -> {n2}")

    # Check for same Name different ID
    dict_2023_name = {x[1]: x[0] for x in set_2023}
    dict_2024_name = {x[1]: x[0] for x in set_2024}

    common_names = set(dict_2023_name.keys()) & set(dict_2024_name.keys())
    diff_ids = []
    for name in common_names:
        if dict_2023_name[name] != dict_2024_name[name]:
            diff_ids.append((name, dict_2023_name[name], dict_2024_name[name]))

    if diff_ids:
        print("\n--- Same Name but different ID (Name: 2023 -> 2024) ---")
        for name, id1, id2 in sorted(diff_ids):
            print(f"{name}: {id1} -> {id2}")


if __name__ == "__main__":
    check_diff()
