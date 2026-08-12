# 주식 데이터 파이프라인: Polygon API에서 Snowflake까지 (GitHub Actions 자동화)

## 📋 핵심 요약 (Executive Summary)
본 문서에서는 **Polygon.io** (Massive API)로부터 주식 레퍼런스 데이터를 추출(Extract)하고, 동적 스키마 맞춤 및 날짜 파티셔닝(`ds`)으로 변환(Transform)하여, **Snowflake** 클라우드 데이터 웨어하우스(`EJAY_K.PUBLIC.STOCK_TICKERS`)로 적재(Load)하는 자동화 데이터 엔지니어링 파이프라인에 대한 전체 기술 설명을 제공합니다. 

본 파이프라인은 **GitHub Actions**를 통해 100% 자동화되어, 개인 노트북을 켜둘 필요도 없고 수동 DB 관리 없이 매일 클라우드 환경에서 서버리스로 자동 실행됩니다.

---

## 🏗️ 아키텍처 및 데이터 흐름 (Architecture & Data Flow)

```
+---------------------------------+
|  Polygon.io (Massive API)       |
|  /v3/reference/tickers          |
+---------------------------------+
                |
                v  (분당 5회 요청 제한 대응 페이징 처리)
+---------------------------------+
|  Python ETL 파이프라인           |
|  [script.py]                    |
|  - 자동 대기/재시도 (12초 백오프) |
|  - 파티션 날짜 태깅 (ds)          |
|  - 동적 타입 스키마 매핑          |
|  - 멱등성 파티션 덮어쓰기 (Delete) |
+---------------------------------+
                |
                v  (Snowflake 커넥터 executemany 배치 적재)
+---------------------------------+
|  Snowflake 데이터 웨어하우스    |
|  EJAY_K.PUBLIC.STOCK_TICKERS    |
+---------------------------------+
                ^
                |  (매일 UTC 01:00 클라우드 자동 실행)
+---------------------------------+
|  GitHub Actions 클라우드 러너   |
|  [.github/workflows/daily...]   |
+---------------------------------+
```

---

## 🛠️ 핵심 기술적 특징 및 해결 과제 (Key Engineering Features)

### 1. API 요청 제한 대응 (API Rate Limiting - 5 Req/Min)
* **문제점**: Polygon.io의 Free/Basic 플랜은 분당 최대 5회의 API 호출만 허용합니다. 대용량 페이징 요청을 연속 실행하면 HTTP 429 에러(`You've exceeded the maximum requests per minute...`)가 발생하여 5,000개 주식 수집 시점에서 작업이 중단되는 현상이 있었습니다.
* **해결책**: `fetch_json_with_retry()` 함수를 구현하여 HTTP 429 상태 코드 및 에러 JSON 발생 시 **12초간 대기(`time.sleep(12)`) 후 자동으로 재시도**하도록 구축했습니다. 이를 통해 14개 페이지에 달하는 13,000여 개의 전체 주식 데이터를 안정적으로 수집합니다.

### 2. 멱등성 보장 및 중복 방지 (Idempotency & Duplicate Prevention)
* **문제점**: 동일한 날짜에 파이프라인을 수동 재실행하거나 여러 번 호출할 경우 데이터가 누적(Append)되어 중복 행이 생성되었습니다 ($13,084 \times 2 = 26,168$행).
* **해결책**: 데이터 적재 직전, 당일 날짜 파티션 데이터를 삭제하는 로직을 추가했습니다:
  ```sql
  DELETE FROM "STOCK_TICKERS" WHERE "DS" = '2026-08-12'
  ```
  이를 통해 당일에 파이프라인을 몇 번을 실행하더라도 항상 **정확히 1개의 깔끔한 13,084행 데이터셋**만 유지하는 **멱등성(Idempotency)**을 완성했습니다.

### 3. 동적 스키마 및 타임 DDL 자동 생성 (Dynamic Typed DDL)
* **문제점**: 수동 DDL 관리는 번거롭고, 문자열(`VARCHAR`)로만 구성된 스테이징 테이블은 검색 성능 및 저장 효율성이 떨어집니다.
* **해결책**: `script.py` 내에 컬럼 데이터 타입 자동 매핑을 구축했습니다:
  - `ACTIVE` $\rightarrow$ `BOOLEAN`
  - `LAST_UPDATED_UTC` $\rightarrow$ `TIMESTAMP_NTZ`
  - `DS` $\rightarrow$ `DATE`
  - 기타 텍스트 필드 $\rightarrow$ `VARCHAR`
  - `DESCRIBE TABLE`을 사용하여 API에 새로운 컬럼이 추가되더라도 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` 문으로 데이터 베이스를 자동 확장합니다.

### 4. 24/7 클라우드 서버리스 자동화 (GitHub Actions)
* **문제점**: 개인 노트북에서 로컬 스케줄러를 실행하면 노트북을 닫거나 전원을 끌 때 파이프라인이 중단됩니다.
* **해결책**: `.github/workflows/daily_stock_job.yml` 파일을 작성하여 매일 한국 시간 기준 오전 10시 (UTC 01:00)에 GitHub 클라우드 서버에서 파이프라인이 무인으로 자동 실행되도록 설정했습니다.

---

## 📂 프로젝트 구조

```
stock-trading-python-app/
├── .github/
│   └── workflows/
│       └── daily_stock_job.yml   # GitHub Actions 매일 자동화 워크플로우
├── .env                          # 로컬 환경 변수 및 비밀키 (Git 제외)
├── .gitignore                    # 보안 및 캐시 파일 Git 제외 설정
├── PIPELINE_DOCUMENTATION.md     # 영문 상세 문서
├── PIPELINE_DOCUMENTATION_KR.md  # 한글 상세 문서
├── README.md                     # GitHub 메인 포트폴리오 문서
├── requirements.txt              # 파이썬 의존성 패키지 목록
├── scheduler.py                  # 로컬 테스트용 데몬 스케줄러
└── script.py                     # 핵심 ETL 수집 및 적재 파이썬 스크립트
```

---

## 📊 Snowflake 검증 쿼리 (Verification Queries)

```sql
-- 1. 날짜별 파티션 적재 건수 확인
SELECT DS, COUNT(*) 
FROM EJAY_K.PUBLIC.STOCK_TICKERS 
GROUP BY DS 
ORDER BY DS DESC;

-- 2. 최신 주식 데이터 20건 조회
SELECT TICKER, NAME, MARKET, TYPE, ACTIVE, LAST_UPDATED_UTC, DS
FROM EJAY_K.PUBLIC.STOCK_TICKERS 
WHERE DS = CURRENT_DATE()
ORDER BY TICKER 
LIMIT 20;

-- 3. 컬럼 타입 확인
DESCRIBE TABLE EJAY_K.PUBLIC.STOCK_TICKERS;
```
