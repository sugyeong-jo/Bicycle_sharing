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

|column|*etl_ymd|etl_ymd_dateType|weekday|
|---|---|---|---|
|설명|날짜|dateType 날짜|요일|
|type|int|datetime|smallint|
|e.g|20180101|2018-01-01|0|

- column: ``` 'etl_ymd', 'elt_ymd_dateType', 'weekday' ```
- weekday: 월(0), 화(1), ... ,일(6)
- [ ] 행사 (Festival), 공휴일 (Holiday) 컬럼 추가
- [ ] 날씨 (Weather), 기온 (Temp), 미세먼지 (Dust) 컬럼 추가




