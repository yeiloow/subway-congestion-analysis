import requests
from dotenv import load_dotenv
import os

load_dotenv()

my_key = os.getenv("KAKAO_API_KEY")


def get_admin_dong(address, api_key):
    headers = {"Authorization": f"KakaoAK {api_key}"}

    # 1. 주소 -> 좌표(X, Y) 변환
    url_search = "https://dapi.kakao.com/v2/local/search/address.json"
    params_search = {"query": address}
    response_search = requests.get(
        url_search, headers=headers, params=params_search
    ).json()

    if not response_search["documents"]:
        return "주소 확인 불가", None

    # 좌표 추출 (x: 경도, y: 위도)
    x = response_search["documents"][0]["x"]
    y = response_search["documents"][0]["y"]

    # 2. 좌표 -> 행정구역(행정동) 변환
    url_geo = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"
    params_geo = {"x": x, "y": y}
    response_geo = requests.get(url_geo, headers=headers, params=params_geo).json()

    # 행정동(region_type='H') 추출
    for doc in response_geo["documents"]:
        if doc["region_type"] == "H":
            return doc["region_3depth_name"], doc["code"]  # 행정동 명칭, 행정동 코드

    return "행정동 정보 없음", None


if __name__ == "__main__":
    print(get_admin_dong("경기 성남시 분당구 판교역로 166", my_key))
