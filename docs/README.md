# 기술 문서
이 디렉터리는 프로젝트의 구현, 실행 방법, 설계 근거와 검증 범위를 설명한다. 계획과 구현 완료를 구분하며, 성능·복구 보장은 측정 결과가 있는 범위로 한정한다.

- [Spark Bronze 가이드](SPARK_GUIDE.md): Kafka 수집, trigger, checkpoint, Iceberg commit과 저장 구조
- [Silver 변환 명세](SILVER_GUIDE.md): 이벤트 계약 검증, 중복 제거, full refresh와 증분 전환 계획
- [변경 기록](CHANGELOG.md): 기능·계약·운영 변경과 검증 결과
- [트러블슈팅](TROUBLESHOOTING.md): 증상, 원인, 대응 및 검증
- [실행 및 접속 안내](../README.md): 환경 구성, 샘플 생성, SQL 조회

## 구성 요소의 선택 이유

- Transactional outbox: 원본 변경과 비즈니스 이벤트를 같은 MySQL 트랜잭션에 기록해 이중 쓰기의 불일치 위험을 줄인다. CDC의 재전달 가능성은 downstream 중복 제거로 다룬다.
- Spark: Kafka micro-batch 적재와 SQL 배치 변환을 같은 엔진으로 구성한다. 현재 local[2]이며 분산 클러스터 성능을 검증한 구성은 아니다.
- Iceberg + MinIO: 객체 저장소의 파일을 snapshot·스키마·원자적 commit을 가진 분석 테이블로 관리한다. Bronze 원문과 Silver 파생 결과를 분리해 재처리 근거를 보존한다.
- PostgreSQL JDBC Catalog: 운영 MySQL과 catalog의 프로세스·볼륨을 분리한다. 별도 컨테이너 운영 비용이 있으며 PostgreSQL의 성능 우위를 실측해 선택한 것은 아니다. 파일 본문과 Iceberg metadata 파일은 MinIO에 있다.
- Spark Thrift Server: 기존 Spark SQL을 JDBC 클라이언트에 제공한다. 로컬 무인증 설정이며 Spark 3.5.6의 JDBC 탐색기 메타데이터는 별도 Iceberg catalog 탐색에 제약이 있다.
- Silver full refresh: 전체 입력으로 기준 결과를 확보하는 초기 구현이다. 대량 데이터의 반복 읽기·쓰기 비용을 줄이기 위한 증분 전환은 후속 계획이며, 두 방식의 결과 일치와 자원 비용을 비교한다.

## 현재 검증 범위

기존 29건 P0의 MySQL/outbox/Kafka/Bronze 정합성과 Silver 정제·재실행 결과를 검증했다. 현재 계약은 예약 10건, 결제 거래 13건, 이벤트 40건과 미수·일반 환불·재수납 필요 환불 구분으로 갱신했으며 새 스키마의 end-to-end 재검증은 아직 수행하지 않았다. 지속 유입 중 장애 복구, 다중 노드 부하, Silver 증분 처리와 Gold 모델은 완료된 성과로 간주하지 않는다. 상세 결과와 당시 조건은 변경 기록을 참조한다.
