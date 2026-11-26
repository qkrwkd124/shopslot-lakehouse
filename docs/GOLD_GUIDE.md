# Gold 가이드

## 매장별 일일 예약 지표

`shop_daily_booking_metrics`는 매장별 일일 예약 생성·취소 건수를 보여준다. 한 행은 **매장 한 곳의 하루**이며, `(shop_id, report_date)` 조합은 유일해야 한다.

| 컬럼 | 의미 |
| --- | --- |
| `shop_id` | 매장 식별자 |
| `shop_name` | `dim_shops`의 매장 이름 |
| `report_date` | 집계 기준일 |
| `created_count` | 해당 날짜에 발생한 `booking_created` 이벤트 건수 |
| `cancelled_count` | 해당 날짜에 발생한 `booking_cancelled` 이벤트 건수 |

생성과 취소는 `booking_events_clean.event_time`의 날짜로 각각 집계한다. 9월 4일에 생성한 예약을 9월 5일에 취소했다면 9월 4일의 생성과 9월 5일의 취소에 각각 1건이 들어간다.

## 날짜 범위와 0건 처리

- `dim_shops.opened_on`부터 매장별로 날짜 행을 만든다. 오픈일을 포함하고 오픈 전에는 행을 만들지 않는다.
- 집계는 어제까지다. `closed_on`이 어제보다 이르면 폐업일까지 포함하고, 폐업일이 비어 있거나 오늘 이후면 어제까지 집계한다. 오늘이나 이후에 여는 매장은 행이 없다.
- 범위 안의 날짜는 이벤트가 없어도 행을 만들고 두 건수를 `0`으로 표시한다.

Spark SQL에서 `sequence`와 `explode`로 매장별 날짜 행을 만들고, `booking_events_clean`에서 구한 집계를 `shop_id`와 `report_date` 두 컬럼으로 `LEFT JOIN`한다. 집계가 비어 있는 날에는 `coalesce`로 건수를 `0`으로 채운다.

## 기대 결과 예시

B매장이 9월 3일에 열고 9월 5일에 폐업했다고 가정한다. 어제가 9월 6일이고, 예약 1건이 9월 4일에 생성돼 9월 5일에 취소됐다면 결과는 아래와 같다. 9월 2일과 9월 6일에는 B매장 행이 없어야 한다.

| report_date | created_count | cancelled_count |
| --- | ---: | ---: |
| 9월 3일 | 0 | 0 |
| 9월 4일 | 1 | 0 |
| 9월 5일 | 0 | 1 |

이 문서는 목표 요구사항을 기록한다. [dbt 모델](../dbt_shopslot/models/gold/shop_daily_booking_metrics.sql)에 날짜 범위와 0건 처리까지 반영한 뒤, 위 예시와 `(shop_id, report_date)`의 유일성을 확인한다.
