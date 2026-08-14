# 주식 데이터 플랫폼: Polygon API에서 Snowflake까지 (GitHub Actions 자동화)

## 📋 핵심 요약 (Executive Summary)
본 문서에서는 **Polygon.io** (Massive API)로부터 주식 종목 메타데이터 및 일별 거래 가격 데이터(OHLCV)를 추출(Extract)하고, 동적 스키마 맞춤 및 날짜 파티셔닝(`ds`)으로 변환(Transform)하여, **Snowflake** 클라우드 데이터 웨어하우스(`STOCK_TICKERS` & `STOCK_PRICES`)로 적재(Load)하는 자동화 데이터 엔지니어링 플랫폼에 대한 전체 기술 설명을 제공합니다.

본 파이프라인은 **GitHub Actions**를 통해 100% 자동화되어, **일별 주가 데이터 수집(매일)**과 **주간 종목 메타데이터 수집(매주 월요일)**이 클라우드 환경에서 서버리스로 개별 자동 실행됩니다.

---

## 🏗️ 아키텍처 및 데이터 흐름 (Architecture & Data Flow)

```
+----------------------------------------------------------------------+
|                     Polygon.io (Massive API)                         |
|  - 레퍼런스 메타데이터: /v3/reference/tickers                           |
|  - 일별 OHLCV 주가:    /v2/aggs/grouped/locale/us/market/stocks      |
+----------------------------------------------------------------------+
                                  |
                                  v  (API 속도 제한 대응 & 그룹화 엔드포인트)
+----------------------------------------------------------------------+
|                     Python ETL 데이터 파이프라인                     |
|                                                                      |
|  1. script.py        --> 마스터 종목 메타데이터 수집                    |
|  2. fetch_prices.py  --> 일별 전체 주식 거래 가격(OHLCV) 수집          |
|  3. backfill.py      --> 과거 종목 메타데이터 백필                      |
|                                                                      |
|  주요 기능: 자동 대기/재시도 (12초 백오프), 파티션 날짜 태깅(ds),        |
|            동적 타입 스키마 매핑, 멱등성 파티션 덮어쓰기 (Delete)        |
+----------------------------------------------------------------------+
                                  |
                                  v  (Snowflake 커넥터 executemany 배치 적재)
+----------------------------------------------------------------------+
|                     Snowflake 데이터 웨어하우스                        |
|                                                                      |
|  - 디멘전 테이블 (Dimension): EJAY_K.PUBLIC.STOCK_TICKERS (메타데이터)  |
|  - 팩트 테이블     (Fact):      EJAY_K.PUBLIC.STOCK_PRICES  (주가 OHLCV) |
+----------------------------------------------------------------------+
       ^                                                ^
       | (주간 주기: 매주 월요일 UTC 01:00)             | (일별 주기: 매일 UTC 01:00)
+----------------------------------------------------------------------+
|                   GitHub Actions 클라우드 러너                        |
|  - .github/workflows/weekly_tickers.yml                              |
|  - .github/workflows/daily_prices.yml                                |
+----------------------------------------------------------------------+
```

---

## 🗓️ 클라우드 스케줄링 주기 (Cloud Schedule)

| 워크플로우 명 | 수집 대상 | 실행 주기 | 실행 시간 (UTC) | Cron 표현식 |
| :--- | :--- | :--- | :--- | :--- |
| **일별 주가 데이터** (`fetch_prices.py`) | 일별 OHLCV 거래 가격 $\rightarrow$ `STOCK_PRICES` | 매일 (Daily) | 01:00 AM UTC | `0 1 * * *` |
| **주간 종목 메타데이터** (`script.py`) | 마스터 종목 레퍼런스 $\rightarrow$ `STOCK_TICKERS` | 매주 (월요일) | 01:00 AM UTC | `0 1 * * 1` |

---

## 💾 Snowflake 이중 테이블 데이터 모델 (Dual Table Model)

| 테이블명 | 엔티티 유형 | 컬럼 / 스키마 | 설명 |
| :--- | :--- | :--- | :--- |
| **`STOCK_TICKERS`** | 디멘전 테이블 (Dimension) | `TICKER`, `NAME`, `MARKET`, `LOCALE`, `PRIMARY_EXCHANGE`, `TYPE`, `ACTIVE`, `CURRENCY_NAME`, `CIK`, `COMPOSITE_FIGI`, `SHARE_CLASS_FIGI`, `LAST_UPDATED_UTC`, `DS` | 주식 메타데이터 마스터 디렉토리 (회사명, 거래소, CIK, 거래 상태 등). 매주 월요일 갱신. |
| **`STOCK_PRICES`** | 팩트 테이블 (Fact) | `TICKER`, `OPEN`, `HIGH`, `LOW`, `CLOSE`, `VOLUME`, `VWAP`, `TRANSACTIONS`, `DS` | 일별 주식 거래 가격 데이터 (시가, 고가, 저가, 종가, 거래량, 거래건수). 매일 갱신. |

---

## 🛠️ 핵심 기술적 특징 및 해결 과제

### 1. 초고속 일별 주가 수집 (하루 1회 API 호출)
* **설계**: Polygon의 그룹화 일별 집계 API (`/v2/aggs/grouped/locale/us/market/stocks/{date}`) 활용.
* **성능**: 하루치 전체 12,000개 주식 가격을 **단 1회의 API 호출(약 3초)** 로 수집. 1개월치 주가 백필도 2분 안에 완료됨.

### 2. API 요청 제한 대응 (API Rate Limiting - 5 Req/Min)
* `fetch_json_with_retry()` 함수가 HTTP 429 에러 및 `"error"` 응답을 감지하여 12초간 자동 대기(`time.sleep(12)`) 후 재시도.

### 3. 멱등성 보장 및 중복 방지 (Idempotent Overwrite)
* 적재 전 해당 날짜 파티션을 삭제하여 중복 방지:
  ```sql
  DELETE FROM "STOCK_TICKERS" WHERE "DS" = '2026-08-12';
  DELETE FROM "STOCK_PRICES" WHERE "DS" = '2026-08-12';
  ```
  동일한 날짜에 몇 번을 재실행하더라도 항상 정확히 1개의 깨끗한 데이터셋만 유지.

### 4. 자가 복구 자동 백필 (Self-Healing Auto-Backfill)
* **설계**: 매일 실행될 때마다 하루치만 수집하는 것이 아니라 최근 3일치 주가 데이터를 자동으로 연속 스캔 및 적재.
* **장점**: 일시적인 네트워크 순단이나 주말 휴장일 전후에도 데이터 누락 없이 공백을 스스로 메우는 완전 무인 시스템 구현.


---

## 📂 프로젝트 구조

```
stock-trading-python-app/
├── .github/
│   └── workflows/
│       ├── daily_prices.yml      # 일별 주가 데이터 GitHub Actions 워크플로우
│       └── weekly_tickers.yml    # 주간 종목 메타데이터 GitHub Actions 워크플로우
├── .env                          # 로컬 환경 변수 및 비밀키 (Git 제외)
├── .gitignore                    # 보안 및 캐시 파일 Git 제외 설정
├── PIPELINE_DOCUMENTATION.md     # 영문 상세 문서
├── PIPELINE_DOCUMENTATION_KR.md  # 한글 상세 문서
├── README.md                     # GitHub 메인 포트폴리오 문서
├── requirements.txt              # 파이썬 의존성 패키지 목록
├── backfill.py                   # 과거 종목 메타데이터 백필 스크립트
├── fetch_prices.py               # 일별 및 과거 주가 수집 스크립트
├── scheduler.py                  # 로컬 테스트용 데몬 스케줄러 (주간/일별 설정)
└── script.py                     # 종목 메타데이터 수집 파이썬 스크립트
```

---

## 📊 Snowflake 검증 및 조인 쿼리

```sql
-- 1. 날짜별 파티션 적재 건수 확인
SELECT DS, COUNT(*) FROM EJAY_K.PUBLIC.STOCK_TICKERS GROUP BY DS ORDER BY DS DESC;
SELECT DS, COUNT(*) FROM EJAY_K.PUBLIC.STOCK_PRICES GROUP BY DS ORDER BY DS DESC;

-- 2. 주가 데이터와 메타데이터 조인 조회 (시각화 및 데이터 분석용)
SELECT p.DS, p.TICKER, t.NAME, t.PRIMARY_EXCHANGE, p.CLOSE, p.VOLUME, p.VWAP
FROM EJAY_K.PUBLIC.STOCK_PRICES p
JOIN EJAY_K.PUBLIC.STOCK_TICKERS t ON p.TICKER = t.TICKER
WHERE p.DS = CURRENT_DATE() - 1
  AND t.TYPE = 'CS'
ORDER BY p.VOLUME DESC
LIMIT 20;
```
