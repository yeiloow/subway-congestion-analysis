import pandas as pd

file_name = "서울교통공사_역주소 _20250318.csv"
file_path = "./location/" + file_name

# read csv and remove "역전화번호" column

df = pd.read_csv(file_path, encoding='utf-8')
df = df.drop(columns=['역전화번호'])
df['행정동'] = df['도로명주소'].str.extract(r'\((.*?)\)')

df.to_csv("서울교통공사_역주소 _2026.csv")