# 건물

대지위치, 주용도(주거, 상업, 공공).

건물에 대한 데이터는 공간 기반 정보로는 [v-world gis](https://www.vworld.kr/dtmk/dtmk_ntads_s002.do?svcCde=NA&dsId=18)에서 얻을 수 있음.
속성 단위 정보로는 [건축물대장](https://www.hub.go.kr/portal/opn/tyb/idx-bdrg-ttlldr.do)에서 월단위 데이터가 구 > 동 단위로 제공됨.

- 주용도
- 세대수
- 인접 학교

geopandas 근방 500m 부근 건물들 주용도, 세대수 합산.
지하철역 위도, 경도 표시.
