# Database 설명

## 1. [ kt ] kt 유동인구 데이터 테이블 

```python
sql = "SELECT * FROM kt limit 1;"
df = pd.read_sql(sql, db)
print(df)
```


|column|id|x|y|timezn_cd|m00|...|total|admi_cd|etl_ymd|
|---|---|---|---|---|---|---|---|---|---|
|설명|위치 id|CRS 좌표계|CRS 좌표계|시간|남자 0~9 세|...|총 인원|행정동 코드|날짜|
|type|int|int|int|smallint|smallint|...|smallint|int|int|
|e.g|100198947|5555|3333|0|0|...|123|31140530|20180101|

- column: ``` 'id', 'x', 'y', 'timezn_cd', 'm00', 'm10', 'm15', 'm20', 'm25', 'm30','m35', 'm40', 'm45', 'm50', 'm55', 'm60', 'm65', 'm70', 'f00', 'f10', 'f15', 'f20', 'f25', 'f30', 'f35', 'f40', 'f45', 'f50', 'f55', 'f60', 'f65', 'f70', 'total', 'admi_cd', 'etl_ymd' ```
- 나이대를 좀 더 고려해 볼 수 있음
- 시간별로 단순하게 합친 결과가 총 머문 인구라고 볼 수는 없다. (평균을 내야 할 수 있음)


- CRS 좌표계 -> WGS 좌표계 변환 코드
``` python
from pyproj import Transformer

transproj_eq = Transformer.from_proj(
    '+proj=tmerc     +lat_0=38     +lon_0=128     +k=0.9999     +x_0=400000     +y_0=600000     +ellps=bessel     +towgs84=-115.8,474.99,674.11,1.16,-2.31,-1.63,6.43 +units=m +no_defs',
    'EPSG:4326',
    always_xy=True,
    skip_equivalent=True)
```
- 자세한 설명은 onedrive 링크 참조
> https://unistackr0-my.sharepoint.com/:f:/g/personal/sjkweon_unist_ac_kr/Ep24d6BcNhJKlqjIjbNA3GkBt58WfTud8GZYfWIn_qPLzg?e=b8nysF


## 2. [ id_WGS ] 위치 id - WGS 좌표계 연결 테이블
|column|*id|wgs_lat|wgs_lat|
|---|---|---|---|
|설명|위치 id|WGS 좌표계|WGS 좌표계|
|type|int|int|int|
|e.g|100198947|35.xxxx|128.xxxx|

- column: ``` 'id', 'wgs_lat', 'wgs_lon' ```

## 3. [ etl_ymd ] 날짜 관련 테이블

|column|*etl_ymd|etl_ymd_dateType|weekday|holiday|holiday_nm|
|---|---|---|---|---|---|
|설명|날짜|dateType 날짜|요일|
|type|int|datetime|smallint|smallint|varchar(10)|
|e.g|20180101|2018-01-01|0|1|신정|

- column: ``` 'etl_ymd', 'elt_ymd_dateType', 'weekday' ```
- weekday: 월(0), 화(1), ... ,일(6)
- [x] 공휴일 (Holiday) 컬럼 추가 
- [ ] 행사 (Event), 
- [x] 날씨 (Weather), 기온 (Temp), 미세먼지 (Dust) 컬럼 추가

## 4. [ etl_ymd_weather ]
|column|*etl_ymd|etl_ymd_dateType|weekday|max_temp|min_temp|pm10_max|pm10_min|zungu_max|zungu_min|
|---|---|---|---|---|---|---|---|---|---|
|설명|날짜|dateType 날짜|요일|최고 기온|최저기온|삼산 최고 미세먼지|삼산 최저 미세먼지|무거동 최대 미세먼지|무거동 최저 미세먼지
|type|int|datetime|smallint|smallint|varchar(10)|decimal(10,0)|decimal(10,0)|varchar(10)|varchar(10)|varchar(10)|varchar(10)|
|e.g|20180101|2018-01-01|0|5|-0.6|142|45|142|45|

- zungu -> 무거동을 의미함
- 미세먼지에 nan 값이 있음

## 5. [ RN_geo ]

|column|rn|*rn_cd|geometry|
|---|---|---|---|
|설명|도로명|도로명 코드|linestring geometry 정보|
|type|varchar(20)|int|varchar(84)|
|e.g|유니스트길|01000026|0 LINSTRING ((125.xx,35.xx),...)|
- 도로명 코드와 geometry 정보
- [ ] geometry정보를 sql로 저장하는 것 알아보기

## 6. [ id_RN ]

|column|*id|rn_cd|
|---|---|---|---|
|설명|위치 id|도로명 코드|
|type|varchar(20)|int|varchar(84)|
|e.g|10019847|1000026|
- 위치 id와 가장 가까운 도로명 할당
