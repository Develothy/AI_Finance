# Alchemetric

> *Alchemy + Metric* — 시장 데이터를 가치 있는 투자 시그널로 변환하는 AI 기반 퀀트 트레이딩 플랫폼

한국·미국 주식 시장의 가격 데이터, 재무제표, 거시경제 지표, 뉴스 센티먼트, DART 공시, 수급 데이터까지 수집하고, 머신러닝·딥러닝 분석을 거쳐 최종 매매 시그널을 생성하는 개인 프로젝트입니다.

## Tech Stack

| 계층 | 기술 |
|------|------|
| Backend | FastAPI, Uvicorn |
| Database | PostgreSQL 15 (운영) / SQLite (개발) |
| ORM | SQLAlchemy 2.0+ |
| ML / DL / RL | scikit-learn, XGBoost, LightGBM, Optuna, PyTorch, stable-baselines3, Gymnasium |
| NLP | transformers, KR-FinBert-SC (한국어 금융 센티먼트) |
| Dashboard | Streamlit, Plotly |
| Scheduling | APScheduler (Step 기반 파이프라인, 10단계 핸들러) |
| Deploy | Docker, docker-compose |

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              EXTERNAL DATA SOURCES                              │
├──────────┬──────────┬───────────┬──────────┬──────────┬───────────┬─────────────┤
│ yfinance │ FDR/FRED │  KIS API  │ DART API │  Naver   │    KRX    │   Google    │
│ (주가+    │ (KR3Y+   │ ( 펀더멘털+  │ (재무+공시)│  News    │  (수급:    │   Trends    │
│  거시7)   │  거시3)   │   수급)     │          │(센티먼트)  │  KIS API) │  +커뮤니티    │
└──────┬───┴────┬─────┴─────┬─────┴─────┬─────┴────┬─────┴─────┬────┴──────┬──────┘
       └────────┴───────────┴───────────┴──────────┴───────────┴───────────┘
                                 │
                ┌────────────────┼───────────────┐
                ▼                ▼               ▼
      ┌──────────────────┐ ┌──────────────┐ ┌────────────────┐
      │    M0: 데이터 수집  │ │   M1: 데이터   │ │    M2: 피처     │
      │    (원시 데이터)    │ │   분석 (NLP)  │ │     엔지니어링    │
      └────────┬─────────┘ └──────┬───────┘ └───────┬────────┘
               └──────────────────┴─────────────────┘
                                │
                      ┌─────────▼─────────┐
                      │   PostgreSQL 15   │ :5432
                      │   feature_store   │
                      └─────────┬─────────┘
                                │
                      ┌─────────▼─────────┐
                      │  M3: ML+DL+RL     │
                      │  (학습/예측)        │
                      └─────────┬─────────┘
                                │
                      ┌─────────▼─────────┐
                      │   M5: 백테스팅      │
                      └─────────┬─────────┘
                                │
                ┌───────────────┼───────────────┐
                ▼               ▼               ▼
      ┌────────────────┐ ┌─────────────┐ ┌──────────────┐
      │   M6: 매매*     │ │   M7: User  │ │  M8: Admin   │
      │   (모의+실매매)   │ │  Dash :8501 │ │  Dash :8502  │
      └────────────────┘ └─────────────┘ └──────────────┘

   * = 개발 예정   (M5 완료)
```

## Modules

| No. | 모듈 | 설명 | 상태 |
|-----|------|------|------|
| 0 | 데이터 수집 | 주가, 펀더멘털, 거시경제, 뉴스, DART 공시, KRX 수급, 대안 데이터 | ✅ 완료 |
| 1 | 데이터 분석 | 뉴스 NLP (KR-FinBert-SC), DART 공시 센티먼트, 대안 데이터 | ✅ 완료 |
| 2 | 피처 엔지니어링 | 기술적 지표 + Phase 1~7 피처 생성 (75) | ✅ 완료 |
| 3 | 머신러닝 | RF / XGBoost / LightGBM, Optuna 튜닝 | ✅ 완료 |
| 4 | 딥러닝 + 강화학습 | LSTM, Transformer (Phase 8A), DQN, PPO (Phase 8B) | ✅ 완료 |
| 5 | 백테스팅/포트폴리오 | 시그널 앙상블 백테스트, 성과 지표, 벤치마크 비교, 모델/종목 레이스 | ✅ 완료 |
| 6 | 매매 시스템 | 모의투자 + 실매매 (KIS/Alpaca) | 🔲 예정 |
| 7 | 사용자 대시보드 | 시장 현황, 종목 분석, 뉴스 센티먼트 (Streamlit :8501) | ✅ 완료 |
| 8 | 관리자 대시보드 | 시스템 모니터링, ML/뉴스/공시/수급 관리 (Streamlit :8502) | ✅ 완료 |
| 9 | 리포트/구독 | LLM 시장 리포트 자동 생성, 이메일/카카오톡/Slack 구독 발송 | 🔲 예정 |

## Data Flow

```
[ 외부 소스 ]
 yfinance ─┬─ 주가 (KR/US)
           ├─ 거시지표 7개 (환율,VIX,KOSPI,S&P500,WTI,Gold,US10Y)
 FDR ──────┤─ KR 3Y 국채금리
 FRED ─────┤─ 거시지표 3개 (기준금리,달러인덱스,CPI)
 KIS API ──┤─ 일별 펀더멘털 (PER,PBR,EPS,외국인비율)
           ├─ 투자자별 매매 (거래대금,매수/매도)
           ├─ 시장별 투자자매매동향 (KOSPI/KOSDAQ)
           ├─ KRX 수급 (공매도,프로그램매매)
 DART API ─┤─ 분기 재무제표 (ROE,부채비율,매출)
           ├─ 공시 목록 + 센티먼트
 Naver ────┤─ 뉴스 센티먼트 (KR-FinBert-SC)
           │
           ▼
[ Module 0: 데이터 수집 ]
           │
           ▼
[ PostgreSQL ]
 stock_price · stock_info · stock_fundamental
 financial_statement · macro_indicator · news_sentiment
 dart_disclosure · krx_supply_demand · alternative_data
           │
           ▼
[ Module 1: 데이터 분석 ]
 뉴스 NLP · DART 공시 센티먼트 · 대안 데이터*
           │
           ▼
[ Module 2: 피처 엔지니어링 ]
 Phase1~7(75) Features — 2-Pass 아키텍처
           │
           ▼
[ feature_store ]
           │
     ┌─────┴─────┐
     ▼           ▼
[ Module 3 ]    [ Module 4 ]
 RF/XGB/LGBM    LSTM/Transformer
 Optuna 튜닝    DQN/PPO (RL)
     │               │
     └─────┬─────────┘
           ▼
[ ml_prediction: BUY / SELL / HOLD ]
           │
           ▼
[ Module 5: 백테스팅 ]
 시그널 앙상블 · 포트폴리오 시뮬레이션
           │
     ┌─────┴─────┐
     ▼           ▼
[ Module 6* ]    [ Module 7/8 ]
 모의+실매매     대시보드

* = 개발 예정 (M5 완료)
```

## [M2] Feature Engineering (7-Phase)

| Phase | 카테고리           | 피처 수 | 상태 |
|-------|-------------------|--------|------|
| 1     | 기술적 지표         | 24     | ✅   |
| 2     | 펀더멘털           | +9     | ✅   |
| 3     | 거시경제           | +11    | ✅   |
| 4     | 뉴스 센티먼트       | +5     | ✅   |
| 5     | DART 공시 + 수급   | +10    | ✅   |
| 6     | 섹터/상대강도 + 뉴스 정제 | +10    | ✅   |
| 7     | 대안 데이터         | +6     | ✅   |
|       | **합계**           | **75** |  |

### Phase 5 피처 상세

| 피처명 | 소스 | 설명 |
|--------|------|------|
| `disclosure_count_30d` | DART | 30일간 공시 건수 |
| `days_since_disclosure` | DART | 마지막 공시 이후 경과일 |
| `disclosure_sentiment` | DART | 공시 센티먼트 점수 |
| `disclosure_type_score` | DART | 공시 유형별 가중 점수 |
| `disclosure_volume_change` | DART | 공시 전후 거래량 변화 |
| `short_selling_volume` | KRX | 공매도 거래량 |
| `short_selling_ratio` | KRX | 공매도 비율 |
| `program_buy_volume` | KRX | 프로그램 매수량 |
| `program_sell_volume` | KRX | 프로그램 매도량 |
| `program_net_volume` | KRX | 프로그램 순매수량 |

### Phase 5.5 — 시장 수급 데이터 확장

| 항목 | API | 상태 |
|------|-----|------|
| 종목별 투자자 거래대금 (순매수/매수/매도) | `FHKST01010900` 필드 확장 | ✅ |
| 시장 전체 투자자별 순매수 수량/거래대금 | `FHPTJ04040000` 신규 | ✅ |
| 당일 거래대금 중 투자자별 비중(%) | 위 데이터 기반 계산 | 🔶 구현 중 |

**종목별 확장 (`FHKST01010900` — 기존 3필드 → 12필드)**

| 필드 | 설명 | 기존 |
|------|------|:----:|
| `prsn_ntby_qty` / `frgn_ntby_qty` / `orgn_ntby_qty` | 순매수 수량 | ✅ |
| `prsn_ntby_tr_pbmn` / `frgn_ntby_tr_pbmn` / `orgn_ntby_tr_pbmn` | 순매수 거래대금 | 🆕 |
| `prsn_shnu_vol` / `frgn_shnu_vol` / `orgn_shnu_vol` | 매수 거래량 | 🆕 |
| `prsn_seln_vol` / `frgn_seln_vol` / `orgn_seln_vol` | 매도 거래량 | 🆕 |

**시장 전체 (`FHPTJ04040000` — 신규)**

| 필드 | 설명 |
|------|------|
| `frgn_ntby_qty` / `prsn_ntby_qty` / `orgn_ntby_qty` | 외국인/개인/기관 순매수 수량 |
| `frgn_ntby_tr_pbmn` / `prsn_ntby_tr_pbmn` / `orgn_ntby_tr_pbmn` | 외국인/개인/기관 순매수 거래대금 |

### Phase 6 피처 상세

**Phase 6A — 섹터/상대강도 (7피처)**

| 피처명 | 설명 |
|--------|------|
| `sector_return_1d` | 동일 섹터 종목 평균 1일 수익률 |
| `sector_return_5d` | 동일 섹터 종목 평균 5일 수익률 |
| `relative_strength_1d` | 종목 1일 수익률 − 섹터 평균 (상대강도) |
| `relative_strength_5d` | 종목 5일 수익률 − 섹터 평균 |
| `relative_strength_20d` | 종목 20일 수익률 − 섹터 평균 |
| `sector_momentum_rank` | 섹터 내 모멘텀 순위 (0~1 정규화) |
| `sector_breadth` | 섹터 내 상승 종목 비율 |

**Phase 6B — 뉴스 센티먼트 정제 (3피처)**

| 피처명 | 설명 |
|--------|------|
| `news_relevance_ratio` | 종목 직접 관련 뉴스 비율 (종목 뉴스 / 전체 뉴스) |
| `news_sentiment_filtered` | 직접 관련 뉴스만으로 재계산한 센티먼트 |
| `sector_news_sentiment` | 동일 섹터 전체 뉴스 평균 센티먼트 |

> Phase 6는 2-pass 아키텍처로 동작: Pass 1에서 Phase 1~5 피처 계산 후 feature_store 저장, Pass 2에서 저장된 peer 데이터를 활용해 섹터/상대강도 피처를 계산합니다.

### Phase 7 피처 상세

| 피처명 | 소스 | 설명 |
|--------|------|------|
| `google_trend_score` | Google Trends | 트렌드 점수 (0~1 정규화) |
| `google_trend_momentum` | Google Trends | 트렌드 모멘텀 (현재 vs 5일전) |
| `community_post_volume` | 네이버 종목토론방 | 게시글 수 (log1p) |
| `community_comment_volume` | 네이버 종목토론방 | 댓글 수 (log1p) |
| `community_engagement_ratio` | 네이버 종목토론방 | 참여도 (댓글/게시글) |
| `alternative_activity_index` | 통합 | 트렌드+커뮤니티 통합 활성도 (0~1) |

### Known Issues

- **뉴스 센티먼트 종목/시장 구분 문제**: 네이버 검색 키워드 기반 `code` 할당으로 종목 센티먼트와 시장 센티먼트가 혼재. Phase 6B `news_relevance_ratio`로 직접 관련 뉴스를 필터링하여 부분 개선됨.
- **종목 간 뉴스 상관관계**: Phase 6B `sector_news_sentiment`로 동일 섹터 뉴스 평균을 피처로 반영하여 개선됨.

## Project Structure

```
AI_Finance/
├── app/                          # Backend API
│   ├── api/                      # FastAPI routes
│   │   ├── main.py               # App entrypoint
│   │   ├── routes/               # Endpoint routers
│   │   │   ├── stock.py          # 주가 API
│   │   │   ├── fundamental.py    # 펀더멘털 API
│   │   │   ├── macro.py          # 거시경제 API
│   │   │   ├── news.py           # 뉴스 센티먼트 API
│   │   │   ├── disclosure.py     # DART 공시 + KRX 수급 API
│   │   │   ├── ml.py             # ML 파이프라인 API
│   │   │   ├── backtest.py       # 백테스트 API
│   │   │   └── admin.py          # 관리자 API
│   │   └── schemas.py            # Pydantic schemas
│   ├── data_collector/           # Module 0: 데이터 수집
│   │   ├── pipeline.py           # 주가 수집 파이프라인
│   │   ├── macro_fetcher.py      # 거시경제 지표 수집 (yfinance+FRED)
│   │   ├── news_fetcher.py       # Naver 뉴스 수집
│   │   ├── sentiment_analyzer.py # KR-FinBert-SC 센티먼트
│   │   ├── kis_fetcher.py        # KIS API 펀더멘털
│   │   ├── dart_fetcher.py       # DART 재무제표
│   │   ├── disclosure_fetcher.py # DART 공시 센티먼트 분석
│   │   ├── krx_fetcher.py        # KRX 수급 데이터 (KIS API)
│   │   ├── google_trends_fetcher.py  # Google Trends 수집
│   │   └── naver_community_fetcher.py # 네이버 커뮤니티 수집
│   ├── scheduler/                # 스케줄러 모듈
│   │   ├── __init__.py
│   │   └── scheduler.py          # APScheduler 잡 관리 (JobScheduler)
│   ├── indicators/               # 기술적 지표 계산
│   ├── ml/                       # Module 2~4: 피처/ML/DL
│   │   ├── feature_engineer.py   # 피처 엔지니어링 (Phase 1~7, 2-Pass)
│   │   ├── trainer.py            # 모델 학습 (ML + DL 라우팅)
│   │   ├── predictor.py          # 예측 실행 (ML + DL 라우팅)
│   │   ├── tuner.py              # Optuna 하이퍼파라미터 (ML)
│   │   ├── ml_config.yaml        # ML/DL/RL 알고리즘 설정
│   │   ├── signal_generator.py   # BUY/SELL/HOLD 시그널
│   │   ├── backtester.py         # Module 5: 백테스팅 엔진
│   │   ├── deep_learning/        # Module 4a: 딥러닝
│   │   │   ├── architectures.py  # LSTM, Transformer 모델 아키텍처
│   │   │   ├── dataset.py        # 시퀀스 데이터셋 생성
│   │   │   ├── dl_trainer.py     # DL 학습 파이프라인
│   │   │   ├── dl_tuner.py       # DL Optuna 하이퍼파라미터
│   │   │   └── dl_predictor.py   # DL 예측
│   │   └── reinforcement/        # Module 4b: 강화학습
│   │       ├── environment.py    # StockTradingEnv (Gymnasium)
│   │       ├── rl_trainer.py     # RL 학습 (DQN/PPO, SB3)
│   │       ├── rl_predictor.py   # RL 예측 + 시그널 변환
│   │       └── rl_tuner.py       # RL Optuna 하이퍼파라미터
│   ├── models/                   # SQLAlchemy 모델
│   │   └── schedule.py           # ScheduleJob + JobStep
│   ├── repositories/             # DB CRUD
│   │   └── scheduler_repository.py  # 스케줄러 CRUD
│   ├── services/                 # 비즈니스 로직
│   ├── core/                     # 유틸리티 (로깅, 예외, 데코레이터)
│   ├── config.py                 # 설정 (환경변수)
│   └── run.py                    # Uvicorn 실행
├── dashboard/                    # Module 7: 사용자 대시보드
│   ├── app.py
│   └── pages/
│       ├── market_overview.py    # 시장 개요
│       ├── stock_analysis.py     # 종목 분석
│       ├── news_sentiment.py     # 뉴스 센티먼트 분석
│       └── sector_view.py        # 섹터 분석
├── admin/                        # Module 8: 관리자 대시보드 (12페이지)
│   ├── app.py
│   └── pages/
│       ├── server_status.py      # 서버 상태
│       ├── db_status.py          # DB 상태/통계
│       ├── log_viewer.py         # 로그 뷰어
│       ├── config_viewer.py      # 설정 조회
│       ├── scheduler_manager.py  # 스케줄러 관리
│       ├── fundamental_manager.py # 재무 데이터 관리
│       ├── news_manager.py       # 뉴스 수집/조회/현황
│       ├── disclosure_manager.py # DART 공시 + KRX 수급 관리
│       ├── ml_train_manager.py   # ML 학습 관리
│       ├── ml_models.py          # 학습된 모델 조회
│       ├── ml_predictions.py     # 예측 테스트
│       └── race.py               # 모델/종목 레이스 (이모지 레이싱 애니메이션)
├── scripts/                         # 유틸리티 스크립트
│   └── migrate_boolean_to_steps.py  # boolean→Step 마이그레이션
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

# FRED (Phase 3 거시경제)
FRED_API_KEY=

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
POST /stocks/collect                  # 주가 데이터 수집
POST /macro/collect                   # 거시경제 지표 수집
POST /news/collect                    # 뉴스 센티먼트 수집
GET  /news/articles                   # 뉴스 기사 조회
GET  /news/sentiment/{code}           # 종목 센티먼트 요약
POST /disclosure/collect              # DART 공시 수집
GET  /disclosure/list                 # 종목별 공시 목록
POST /disclosure/supply/collect       # KRX 수급 데이터 수집
GET  /disclosure/supply/{market}/{code} # 종목별 수급 시계열
POST /ml/features/compute             # 피처 계산
POST /ml/train                        # 모델 학습
POST /ml/predict/{code}               # 종목 예측
GET  /ml/models                       # 모델 목록
GET  /ml/predictions                  # 예측 결과
POST /backtest/run                    # 백테스트 실행
GET  /backtest/runs                   # 백테스트 이력
GET  /backtest/runs/{id}              # 백테스트 상세
GET  /backtest/runs/{id}/trades       # 거래 로그
GET  /backtest/runs/{id}/equity       # 에쿼티 커브
DELETE /backtest/runs/{id}            # 백테스트 삭제
POST /backtest/compare                # 복수 백테스트 비교
POST /backtest/race/model             # 모델 레이스 (모델별 개별 백테스트 경주)
POST /backtest/race/stock             # 종목 레이스 (종가 수익률 경주)
GET  /backtest/race/{race_group}      # 레이스 결과 재조회
GET  /admin/health                    # 헬스체크
```

## Roadmap

- [x] Phase 1 — 데이터 수집 인프라 + ML Phase1 (24피처)
- [x] Phase 2 — 펀더멘털 통합 + ML Phase2 (33피처) + 관리자 대시보드
- [x] Phase 3 — 거시경제 지표 + ML Phase3 (41피처)
- [x] Phase 4 — 뉴스 센티먼트 수집/분석 (Naver API + KR-FinBert-SC, 49피처)
- [x] Phase 4.5 — 뉴스 센티먼트 대시보드 (사용자: 센티먼트 분석, 관리자: 수집/조회/현황)
- [x] Phase 5 — DART 공시 + KRX 수급 데이터 (KIS API, 59피처) + 어드민 대시보드
- [x] Phase 5.5 — 시장 수급 데이터 확장 (투자자별 거래대금 + 시장 전체 매매동향) + 일괄 수집 스케줄러
- [x] Phase 6 — 데이터 개선: 섹터/상대강도 + 뉴스 센티먼트 정제 (69피처, 2-pass 아키텍처)
- [x] Phase 7 — 대안 데이터 (Google Trends, 커뮤니티 활성도, 75피처)
- [x] Phase 8A — 딥러닝 시계열 분류 (LSTM, Transformer + Optuna DL 튜닝)
- [x] Phase 8B — 강화학습 (DQN, PPO 매매 에이전트, Gymnasium 환경, Optuna RL 튜닝)

> **Phase 8A vs 8B 비교**
>
> | | Phase 8A (지도학습) | Phase 8B (강화학습) |
> |---|---|---|
> | **목적** | "내일 오를까?" 분류 | "언제 사고 팔까?" 매매 전략 |
> | **방식** | 지도학습 (정답 레이블) | 강화학습 (보상 기반 탐색) |
> | **출력** | 확률 (UP 62%) | 행동 (BUY / SELL / HOLD) |
> | **모델** | LSTM, Transformer | DQN, PPO (신경망 + RL) |
> | **학습** | 과거 데이터 한 번 | 환경과 반복 상호작용 |
- [x] Phase 9 — 백테스팅 (시그널 앙상블 4방식, 포트폴리오 시뮬레이션, 성과 지표, Buy & Hold 벤치마크, 모델/종목 레이스)
- [ ] Phase 10 — 매매 시스템 (모의투자 + 실매매)
- [ ] Phase 11 — 리포트 생성 + 구독 서비스 (ML+DL 통합 시그널 + 이메일/카카오톡/Slack 발송)
