def generate_year_code(start_year, end_year):
    compact_list = [
        f"{year}{q}" for year in range(start_year, end_year + 1) for q in range(1, 5)
    ]
    return compact_list
