# Alchemetric

> *Alchemy + Metric* — 시장 데이터를 가치 있는 투자 시그널로 변환하는 AI 기반 퀀트 트레이딩 플랫폼

한국·미국 주식 시장의 가격 데이터, 재무제표, 거시경제 지표, 뉴스 센티먼트까지 수집하고, 머신러닝·딥러닝 분석을 거쳐 최종 매매 시그널을 생성하는 개인 프로젝트입니다.

## Tech Stack

| 계층 | 기술 |
|------|------|
| Backend | FastAPI, Uvicorn |
| Database | PostgreSQL 15 (운영) / SQLite (개발) |
| ORM | SQLAlchemy 2.0+ |
| ML | scikit-learn, XGBoost, LightGBM, Optuna |
| NLP | transformers, KR-FinBert-SC (한국어 금융 센티먼트) |
| Dashboard | Streamlit, Plotly |
| Scheduling | APScheduler |
| Deploy | Docker, docker-compose |

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                   EXTERNAL DATA SOURCES                  │
├──────────┬──────────┬──────────┬──────────┬──────────────┤
│ yfinance │   FDR    │ KIS API  │ DART API │ Naver News.  │
│ (주가+    │  (KR3Y)  │ (펀더멘털) │ (재무+공시) │ (뉴스 센티먼트) │
│  거시7)   │          │          │          │              │
└──────┬───┴────┬─────┴────┬─────┴────┬─────┴──────┬───────┘
       └────────┴──────────┴──────────┴────────────┘
                             │
             ┌───────────────┼───────────────┐
             ▼               ▼               ▼
   ┌──────────────────┐ ┌──────────────┐ ┌────────────────┐
   │    M0: 데이터 수집  │ │   M1: 데이터   │ │    M2: 피처     │
   │    (원시 데이터)    │ │   분석 (NLP)* │ │     엔지니어링    │
   └────────┬─────────┘ └──────┬───────┘ └───────┬────────┘
            └──────────────────┴─────────────────┘
                             │
                   ┌─────────▼─────────┐
                   │   PostgreSQL 15   │ :5432
                   │   feature_store   │
                   └─────────┬─────────┘
                             │
                   ┌─────────▼─────────┐
                   │  M3: ML / M4: DL* │
                   │  (학습/예측)        │
                   └─────────┬─────────┘
                             │
                   ┌─────────▼─────────┐
                   │   M5: 백테스팅*     │
                   └─────────┬─────────┘
                             │
             ┌───────────────┼───────────────┐
             ▼               ▼               ▼
   ┌────────────────┐ ┌─────────────┐ ┌──────────────┐
   │   M6: 매매*     │ │   M7: User  │ │  M8: Admin   │
   │   (모의+실매매)   │ │  Dash :8501 │ │  Dash :8502  │
   └────────────────┘ └─────────────┘ └──────────────┘

   * = 개발 예정
```

## Modules

| No. | 모듈 | 설명 | 상태 |
|-----|------|------|------|
| 0 | 데이터 수집 | 주가, 펀더멘털, 거시경제, 수급*, 섹터* | ✅ 완료 |
| 1 | 데이터 분석 | 뉴스 NLP (KR-FinBert-SC), DART 공시*, 대안* | 🔶 부분 |
| 2 | 피처 엔지니어링 | 기술적 지표 + Phase 1~4 피처 생성 (49/57~63) | 🔶 부분 |
| 3 | 머신러닝 | RF / XGBoost / LightGBM, Optuna 튜닝 | ✅ 완료 |
| 4 | 딥러닝 | LSTM, Transformer, 강화학습 | 🔲 예정 |
| 5 | 백테스팅/포트폴리오 | 전략 검증, 포트폴리오 최적화 | 🔲 예정 |
| 6 | 매매 시스템 | 모의투자 + 실매매 (KIS/Alpaca) | 🔲 예정 |
| 7 | 사용자 대시보드 | 시장 현황, 종목 분석, 뉴스 센티먼트 (Streamlit :8501) | ✅ 완료 |
| 8 | 관리자 대시보드 | 시스템 모니터링, ML 관리, 뉴스 관리 (Streamlit :8502) | ✅ 완료 |

## Data Flow

```
[ 외부 소스 ]
 yfinance ─┬─ 주가 (KR/US)
           ├─ 거시지표 7개 (환율,VIX,KOSPI,S&P500,WTI,Gold,US10Y)
 FDR ──────┤─ KR 3Y 국채금리
 KIS API ──┤─ 일별 펀더멘털 (PER,PBR,EPS,외국인비율)
 DART API ─┤─ 분기 재무제표 (ROE,부채비율,매출)
 Naver ────┤─ 뉴스 센티먼트 (KR-FinBert-SC)
 KRX* ─────┤─ 수급 (공매도,대차,프로그램매매,신용잔고)
           │
           ▼
[ Module 0: 데이터 수집 ]
           │
           ▼
[ PostgreSQL ]
 stock_price · stock_info · stock_fundamental
 financial_statement · macro_indicator · news_sentiment
           │
           ▼
[ Module 1: 데이터 분석* ]
 뉴스 NLP · DART 공시 · 대안 데이터
           │
           ▼
[ Module 2: 피처 엔지니어링 ]
 Phase1~4(49) + Phase5~8(+8~14*) = 57~63 Features
           │
           ▼
[ feature_store ]
           │
     ┌─────┴─────┐
     ▼           ▼
[ Module 3 ]    [ Module 4* ]
 RF/XGB/LGBM    LSTM/Transformer
 Optuna 튜닝    강화학습
     │               │
     └─────┬─────────┘
           ▼
[ ml_prediction: BUY / SELL / HOLD ]
           │
           ▼
[ Module 5: 백테스팅* ]
           │
     ┌─────┴─────┐
     ▼           ▼
[ Module 6* ]    [ Module 7/8 ]
 모의+실매매     대시보드

* = 개발 예정
```

## Feature Engineering (8-Phase)

| Phase | 카테고리       | 피처 수 | 상태 |
|-------|---------------|--------|------|
| 1     | 기술적 지표     | 24     | ✅   |
| 2     | 펀더멘털       | +9     | ✅   |
| 3     | 거시경제       | +11    | ✅   |
| 4     | 뉴스 센티먼트   | +5     | ✅   |
| 5     | DART 공시      | +5~7   | 🔲   |
| 6     | 수급 데이터     | +4~5   | 🔲   |
| 7     | 대안 데이터     | +2~3   | 🔲   |
| 8     | 섹터/상대강도   | +2     | 🔲   |
|       | **합계 (현재 49)** | **57~63** |  |

### Known Issues (Phase 5~8에서 개선)

- **뉴스 센티먼트 종목/시장 구분 문제**: 현재 네이버 검색 키워드 기반으로 `code` 할당. "삼성전자 주가"로 검색 시 시장 기사·섹터 기사도 `code="005930"`으로 저장되어 종목 센티먼트와 시장 센티먼트가 혼재됨. 같은 기사가 시장 키워드로도 수집되면 `code=NULL`로 중복 저장 → 이중 카운팅 발생 가능.
- **종목 간 뉴스 상관관계 미반영**: 하이닉스 피처 계산 시 삼성전자 뉴스 미반영 (동일 섹터 영향 무시). Phase 8 섹터/상대강도에서 섹터 평균 센티먼트 등으로 개선 예정.

## Project Structure

```
AI_Finance/
├── app/                          # Backend API
│   ├── api/                      # FastAPI routes
│   │   ├── main.py               # App entrypoint
│   │   ├── routes/               # Endpoint routers
│   │   └── schemas.py            # Pydantic schemas
│   ├── data_collector/           # Module 0: 데이터 수집
│   │   ├── pipeline.py           # 주가 수집 파이프라인
│   │   ├── macro_fetcher.py      # 거시경제 지표 수집
│   │   ├── news_fetcher.py       # Naver 뉴스 수집
│   │   ├── sentiment_analyzer.py # KR-FinBert-SC 센티먼트
│   │   ├── kis_fetcher.py        # KIS API 펀더멘털
│   │   ├── dart_fetcher.py       # DART 재무제표
│   │   └── scheduler.py          # APScheduler 잡 관리
│   ├── indicators/               # 기술적 지표 계산
│   ├── ml/                       # Module 2~3: 피처/ML
│   │   ├── feature_engineer.py   # 피처 엔지니어링
│   │   ├── trainer.py            # 모델 학습
│   │   ├── predictor.py          # 예측 실행
│   │   ├── tuner.py              # Optuna 하이퍼파라미터
│   │   └── signal_generator.py   # BUY/SELL/HOLD 시그널
│   ├── models/                   # SQLAlchemy 모델
│   ├── repositories/             # DB CRUD
│   ├── services/                 # 비즈니스 로직
│   ├── config.py                 # 설정 (환경변수)
│   └── run.py                    # Uvicorn 실행
├── dashboard/                    # Module 7: 사용자 대시보드
│   ├── app.py
│   └── pages/
│       ├── stock_analysis.py     # 종목 분석
│       ├── news_sentiment.py     # 뉴스 센티먼트 분석
│       ├── market_overview.py    # 시장 개요
│       └── sector_view.py        # 섹터 분석
├── admin/                        # Module 8: 관리자 대시보드
│   ├── app.py
│   └── pages/
│       ├── news_manager.py       # 뉴스 수집/조회/현황
│       ├── fundamental_manager.py # 재무 데이터 관리
│       ├── scheduler_manager.py  # 스케줄러 관리
│       └── ml_*.py               # ML 학습/모델/예측
├── docs/                         # 프로젝트 문서
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.dashboard
├── requirements.txt
└── requirements-dashboard.txt
```

## Quick Start

### 1. 환경변수 설정

```bash
cp .env.example .env
```

`.env` 파일에 필요한 키를 입력합니다:

```env
# Database
DB_TYPE=postgresql
DB_HOST=localhost
DB_PORT=5432
DB_NAME=quant_platform
DB_USER=postgres
DB_PASSWORD=quant1234

# KIS API (실전)
KIS_APP_KEY=
KIS_APP_SECRET=
KIS_ACCOUNT_NO=

# KIS API (모의)
KIS_MOCK_APP_KEY=
KIS_MOCK_APP_SECRET=
KIS_MOCK_ACCOUNT_NO=
KIS_MOCK_MODE=true

# DART
DART_API_KEY=

# Naver News API (Phase 4)
NAVER_CLIENT_ID=
NAVER_CLIENT_SECRET=

# Slack (선택)
SLACK_ENABLED=false
SLACK_TOKEN=
SLACK_WEBHOOK_URL=

# App
DEV_MODE=true
LOG_LEVEL=DEBUG
```

### 2. Docker Compose 실행

```bash
docker-compose up -d
```

4개 서비스가 실행됩니다:

| 서비스 | 포트 | 설명 |
|--------|------|------|
| `quant_postgres` | 5432 | PostgreSQL 15 |
| `quant_app` | 8000 | FastAPI Backend |
| `quant_dashboard` | 8501 | 사용자 대시보드 |
| `quant_admin` | 8502 | 관리자 대시보드 |

### 3. 로컬 개발 (Docker 없이)

```bash
pip install -r requirements.txt
python app/run.py
```

## API Endpoints

```
POST /stocks/collect           # 주가 데이터 수집
POST /macro/collect            # 거시경제 지표 수집
POST /news/collect             # 뉴스 센티먼트 수집
GET  /news/articles            # 뉴스 기사 조회
GET  /news/sentiment/{code}    # 종목 센티먼트 요약
POST /ml/features/compute      # 피처 계산
POST /ml/train                 # 모델 학습
POST /ml/predict/{code}        # 종목 예측
GET  /ml/models                # 모델 목록
GET  /ml/predictions           # 예측 결과
GET  /admin/health             # 헬스체크
```

## Roadmap

- [x] Phase 1 — 데이터 수집 인프라 + ML Phase1 (24피처)
- [x] Phase 2 — 펀더멘털 통합 + ML Phase2 (33피처) + 관리자 대시보드
- [x] Phase 3 — 거시경제 지표 + ML Phase3 (41피처)
- [x] Phase 4 — 뉴스 센티먼트 수집/분석 (Naver API + KR-FinBert-SC, 49피처)
- [x] Phase 4.5 — 뉴스 센티먼트 대시보드 (사용자: 센티먼트 분석, 관리자: 수집/조회/현황)
- [ ] Phase 5 — DART 공시 + 수급 데이터
- [ ] Phase 6 — 대안 데이터 + 섹터/상대강도
- [ ] Phase 7 — 백테스팅 + 포트폴리오 최적화
- [ ] Phase 8 — 딥러닝 (LSTM, Transformer, 강화학습)
- [ ] Phase 9 — 매매 시스템 (모의투자 + 실매매) + 대시보드 고도화
