-- ============================================================
-- AI_Finance 전체 마이그레이션 스크립트 (PostgreSQL)
-- 생성일: 2026-03-20
-- 모델 기준: 현재 app/models/ 전체 (20 테이블, 31 모델)
-- 사용법: psql -d finance_db -f migration.sql
-- ============================================================

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 1. 주가 / 종목정보
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CREATE TABLE IF NOT EXISTS stock_price (
    id            SERIAL PRIMARY KEY,
    market        VARCHAR(10)    NOT NULL,
    code          VARCHAR(20)    NOT NULL,
    date          DATE           NOT NULL,
    open          NUMERIC(15,2),
    high          NUMERIC(15,2),
    low           NUMERIC(15,2),
    close         NUMERIC(15,2),
    volume        BIGINT,
    created_at    TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_stock_price UNIQUE (market, code, date)
);
CREATE INDEX IF NOT EXISTS idx_stock_price_lookup ON stock_price (market, code, date);

CREATE TABLE IF NOT EXISTS stock_info (
    id            SERIAL PRIMARY KEY,
    market        VARCHAR(10)    NOT NULL,
    code          VARCHAR(20)    NOT NULL,
    name          VARCHAR(100),
    sector        VARCHAR(50),
    industry      VARCHAR(100),
    updated_at    TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_stock_info UNIQUE (market, code)
);
CREATE INDEX IF NOT EXISTS idx_stock_info_sector ON stock_info (market, sector);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 2. 재무 데이터 (Phase 2)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CREATE TABLE IF NOT EXISTS stock_fundamental (
    id                        SERIAL PRIMARY KEY,
    market                    VARCHAR(10)  NOT NULL,
    code                      VARCHAR(20)  NOT NULL,
    date                      DATE         NOT NULL,
    per                       NUMERIC(10,2),
    pbr                       NUMERIC(10,2),
    eps                       NUMERIC(15,2),
    bps                       NUMERIC(15,2),
    market_cap                BIGINT,
    div_yield                 NUMERIC(8,4),
    foreign_ratio             NUMERIC(8,4),
    inst_net_buy              BIGINT,
    foreign_net_buy           BIGINT,
    individual_net_buy        BIGINT,
    inst_net_buy_amount       BIGINT,
    foreign_net_buy_amount    BIGINT,
    individual_net_buy_amount BIGINT,
    inst_buy_vol              BIGINT,
    foreign_buy_vol           BIGINT,
    individual_buy_vol        BIGINT,
    inst_sell_vol             BIGINT,
    foreign_sell_vol          BIGINT,
    individual_sell_vol       BIGINT,
    created_at                TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_stock_fundamental UNIQUE (market, code, date)
);
CREATE INDEX IF NOT EXISTS idx_fundamental_lookup ON stock_fundamental (market, code, date);

-- Phase 5.5 추가 컬럼 (기존 테이블에 없을 경우)
DO $$ BEGIN
    ALTER TABLE stock_fundamental ADD COLUMN inst_net_buy_amount BIGINT;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
    ALTER TABLE stock_fundamental ADD COLUMN foreign_net_buy_amount BIGINT;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
    ALTER TABLE stock_fundamental ADD COLUMN individual_net_buy_amount BIGINT;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
    ALTER TABLE stock_fundamental ADD COLUMN inst_buy_vol BIGINT;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
    ALTER TABLE stock_fundamental ADD COLUMN foreign_buy_vol BIGINT;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
    ALTER TABLE stock_fundamental ADD COLUMN individual_buy_vol BIGINT;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
    ALTER TABLE stock_fundamental ADD COLUMN inst_sell_vol BIGINT;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
    ALTER TABLE stock_fundamental ADD COLUMN foreign_sell_vol BIGINT;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
    ALTER TABLE stock_fundamental ADD COLUMN individual_sell_vol BIGINT;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;


CREATE TABLE IF NOT EXISTS financial_statement (
    id                SERIAL PRIMARY KEY,
    market            VARCHAR(10)  NOT NULL,
    code              VARCHAR(20)  NOT NULL,
    period            VARCHAR(10)  NOT NULL,
    period_date       DATE         NOT NULL,
    revenue           BIGINT,
    operating_profit  BIGINT,
    net_income        BIGINT,
    roe               NUMERIC(10,2),
    roa               NUMERIC(10,2),
    operating_margin  NUMERIC(10,2),
    net_margin        NUMERIC(10,2),
    debt_ratio        NUMERIC(10,2),
    source            VARCHAR(30)  DEFAULT 'dart',
    created_at        TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_financial_statement UNIQUE (market, code, period)
);
CREATE INDEX IF NOT EXISTS idx_financial_lookup ON financial_statement (market, code, period_date);


CREATE TABLE IF NOT EXISTS market_investor_trading (
    id                        SERIAL PRIMARY KEY,
    market                    VARCHAR(10)  NOT NULL,
    date                      DATE         NOT NULL,
    foreign_net_buy_qty       BIGINT,
    inst_net_buy_qty          BIGINT,
    individual_net_buy_qty    BIGINT,
    foreign_net_buy_amount    BIGINT,
    inst_net_buy_amount       BIGINT,
    individual_net_buy_amount BIGINT,
    created_at                TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_market_investor_trading UNIQUE (market, date)
);
CREATE INDEX IF NOT EXISTS idx_market_investor_lookup ON market_investor_trading (market, date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 3. 거시경제 지표 (Phase 3)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CREATE TABLE IF NOT EXISTS macro_indicator (
    id              SERIAL PRIMARY KEY,
    date            DATE         NOT NULL,
    indicator_name  VARCHAR(50)  NOT NULL,
    value           NUMERIC(15,4) NOT NULL,
    change_pct      NUMERIC(10,6),
    source          VARCHAR(30),
    created_at      TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_macro_indicator UNIQUE (date, indicator_name)
);
CREATE INDEX IF NOT EXISTS idx_macro_lookup ON macro_indicator (indicator_name, date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 4. 뉴스 센티먼트 (Phase 4)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CREATE TABLE IF NOT EXISTS news_sentiment (
    id              SERIAL PRIMARY KEY,
    date            DATE          NOT NULL,
    market          VARCHAR(10)   NOT NULL DEFAULT 'KR',
    code            VARCHAR(20),
    title           VARCHAR(500)  NOT NULL,
    description     TEXT,
    url             VARCHAR(1000),
    source          VARCHAR(30)   DEFAULT 'naver',
    sentiment_score NUMERIC(6,4),
    sentiment_label VARCHAR(20),
    created_at      TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_news_sentiment UNIQUE (url, code)
);
CREATE INDEX IF NOT EXISTS idx_news_lookup    ON news_sentiment (market, code, date);
CREATE INDEX IF NOT EXISTS idx_news_date      ON news_sentiment (date);
CREATE INDEX IF NOT EXISTS idx_news_code_date ON news_sentiment (code, date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 5. 공시 + 수급 (Phase 5)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CREATE TABLE IF NOT EXISTS dart_disclosure (
    id              SERIAL PRIMARY KEY,
    date            DATE          NOT NULL,
    market          VARCHAR(10)   NOT NULL,
    code            VARCHAR(20)   NOT NULL,
    corp_name       VARCHAR(100),
    report_nm       VARCHAR(500)  NOT NULL,
    rcept_no        VARCHAR(20)   NOT NULL,
    flr_nm          VARCHAR(100),
    rcept_dt        VARCHAR(10),
    report_type     VARCHAR(50),
    type_score      NUMERIC(4,2)  DEFAULT 0.2,
    sentiment_score NUMERIC(6,4),
    sentiment_label VARCHAR(20),
    created_at      TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_dart_disclosure UNIQUE (rcept_no, code)
);
CREATE INDEX IF NOT EXISTS idx_disclosure_lookup ON dart_disclosure (market, code, date);
CREATE INDEX IF NOT EXISTS idx_disclosure_date   ON dart_disclosure (date);


CREATE TABLE IF NOT EXISTS krx_supply_demand (
    id                   SERIAL PRIMARY KEY,
    market               VARCHAR(10) NOT NULL,
    code                 VARCHAR(20) NOT NULL,
    date                 DATE        NOT NULL,
    short_selling_volume BIGINT,
    short_selling_ratio  NUMERIC(8,4),
    program_buy_volume   BIGINT,
    program_sell_volume  BIGINT,
    margin_balance       BIGINT,
    source               VARCHAR(30) DEFAULT 'pykrx',
    created_at           TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_krx_supply_demand UNIQUE (market, code, date)
);
CREATE INDEX IF NOT EXISTS idx_supply_demand_lookup ON krx_supply_demand (market, code, date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 6. 대안 데이터 (Phase 7)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CREATE TABLE IF NOT EXISTS alternative_data (
    id                          SERIAL PRIMARY KEY,
    date                        DATE         NOT NULL,
    market                      VARCHAR(10)  NOT NULL,
    code                        VARCHAR(20)  NOT NULL,
    google_trend_value          NUMERIC(6,2),
    google_trend_interpolated   NUMERIC(6,2),
    community_post_count        INTEGER,
    community_comment_count     INTEGER,
    source                      VARCHAR(30)  DEFAULT 'mixed',
    created_at                  TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_alternative_data UNIQUE (market, code, date)
);
CREATE INDEX IF NOT EXISTS idx_alternative_lookup ON alternative_data (market, code, date);
CREATE INDEX IF NOT EXISTS idx_alternative_date   ON alternative_data (date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 7. ML 파이프라인
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CREATE TABLE IF NOT EXISTS feature_store (
    id                          SERIAL PRIMARY KEY,
    market                      VARCHAR(10)  NOT NULL,
    code                        VARCHAR(20)  NOT NULL,
    date                        DATE         NOT NULL,

    -- 가격 피처
    close                       NUMERIC(15,2),
    return_1d                   NUMERIC(10,6),
    return_5d                   NUMERIC(10,6),
    return_20d                  NUMERIC(10,6),
    volatility_20d              NUMERIC(10,6),
    volume_ratio                NUMERIC(10,4),

    -- 기술적 지표
    sma_5                       NUMERIC(15,4),
    sma_20                      NUMERIC(15,4),
    sma_60                      NUMERIC(15,4),
    ema_12                      NUMERIC(15,4),
    ema_26                      NUMERIC(15,4),
    rsi_14                      NUMERIC(8,4),
    macd                        NUMERIC(15,4),
    macd_signal                 NUMERIC(15,4),
    macd_histogram              NUMERIC(15,4),
    bb_upper                    NUMERIC(15,4),
    bb_middle                   NUMERIC(15,4),
    bb_lower                    NUMERIC(15,4),
    bb_width                    NUMERIC(10,6),
    bb_pctb                     NUMERIC(10,6),
    obv                         NUMERIC(20,2),

    -- 파생 피처
    price_to_sma20              NUMERIC(10,6),
    price_to_sma60              NUMERIC(10,6),
    golden_cross                INTEGER,
    rsi_zone                    INTEGER,

    -- 재무 피처 (Phase 2)
    per                         NUMERIC(10,2),
    pbr                         NUMERIC(10,2),
    eps                         NUMERIC(15,2),
    market_cap                  BIGINT,
    foreign_ratio               NUMERIC(8,4),
    inst_net_buy                BIGINT,
    foreign_net_buy             BIGINT,
    roe                         NUMERIC(10,2),
    debt_ratio                  NUMERIC(10,2),

    -- 거시 피처 (Phase 3)
    krw_usd                     NUMERIC(10,4),
    vix                         NUMERIC(8,4),
    kospi_index                 NUMERIC(10,2),
    us_10y                      NUMERIC(8,4),
    kr_3y                       NUMERIC(8,4),
    sp500                       NUMERIC(10,2),
    wti                         NUMERIC(10,2),
    gold                        NUMERIC(10,2),
    fed_rate                    NUMERIC(8,4),
    usd_index                   NUMERIC(10,4),
    us_cpi                      NUMERIC(10,4),

    -- 뉴스 센티먼트 피처 (Phase 4)
    news_sentiment              NUMERIC(6,4),
    news_volume                 INTEGER,
    news_sentiment_std          NUMERIC(6,4),
    market_sentiment            NUMERIC(6,4),
    market_news_volume          INTEGER,

    -- 공시 피처 (Phase 5A)
    disclosure_count_30d        INTEGER,
    days_since_disclosure       INTEGER,
    disclosure_sentiment        NUMERIC(6,4),
    disclosure_type_score       NUMERIC(4,2),
    disclosure_volume_change    NUMERIC(10,4),

    -- 수급 피처 (Phase 5B)
    short_selling_volume        BIGINT,
    short_selling_ratio         NUMERIC(8,4),
    program_buy_volume          BIGINT,
    program_sell_volume         BIGINT,
    program_net_volume          BIGINT,

    -- 섹터/상대강도 피처 (Phase 6A)
    sector_return_1d            NUMERIC(10,6),
    sector_return_5d            NUMERIC(10,6),
    relative_strength_1d        NUMERIC(10,6),
    relative_strength_5d        NUMERIC(10,6),
    relative_strength_20d       NUMERIC(10,6),
    sector_momentum_rank        NUMERIC(6,4),
    sector_breadth              NUMERIC(6,4),

    -- 뉴스 정제 피처 (Phase 6B)
    news_relevance_ratio        NUMERIC(6,4),
    news_sentiment_filtered     NUMERIC(6,4),
    sector_news_sentiment       NUMERIC(6,4),

    -- 대안 데이터 피처 (Phase 7)
    google_trend_score          NUMERIC(6,4),
    google_trend_momentum       NUMERIC(10,6),
    community_post_volume       NUMERIC(10,4),
    community_comment_volume    NUMERIC(10,4),
    community_engagement_ratio  NUMERIC(8,4),
    alternative_activity_index  NUMERIC(8,4),

    -- 타겟 변수
    target_class_1d             INTEGER,
    target_class_5d             INTEGER,
    target_return_1d            NUMERIC(10,6),
    target_return_5d            NUMERIC(10,6),

    created_at                  TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_feature_store UNIQUE (market, code, date)
);
CREATE INDEX IF NOT EXISTS idx_feature_store_lookup ON feature_store (market, code, date);
CREATE INDEX IF NOT EXISTS idx_feature_store_date   ON feature_store (date);

-- feature_store Phase별 ALTER (기존 테이블에 누락된 컬럼 추가)
-- Phase 3 거시 피처
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN us_10y NUMERIC(8,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN kr_3y NUMERIC(8,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN sp500 NUMERIC(10,2); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN wti NUMERIC(10,2); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN gold NUMERIC(10,2); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN fed_rate NUMERIC(8,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN usd_index NUMERIC(10,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN us_cpi NUMERIC(10,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- Phase 4 뉴스 센티먼트
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN news_sentiment NUMERIC(6,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN news_volume INTEGER; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN news_sentiment_std NUMERIC(6,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN market_sentiment NUMERIC(6,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN market_news_volume INTEGER; EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- Phase 5A 공시
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN disclosure_count_30d INTEGER; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN days_since_disclosure INTEGER; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN disclosure_sentiment NUMERIC(6,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN disclosure_type_score NUMERIC(4,2); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN disclosure_volume_change NUMERIC(10,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- Phase 5B 수급
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN short_selling_volume BIGINT; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN short_selling_ratio NUMERIC(8,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN program_buy_volume BIGINT; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN program_sell_volume BIGINT; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN program_net_volume BIGINT; EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- Phase 6A 섹터/상대강도
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN sector_return_1d NUMERIC(10,6); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN sector_return_5d NUMERIC(10,6); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN relative_strength_1d NUMERIC(10,6); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN relative_strength_5d NUMERIC(10,6); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN relative_strength_20d NUMERIC(10,6); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN sector_momentum_rank NUMERIC(6,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN sector_breadth NUMERIC(6,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- Phase 6B 뉴스 정제
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN news_relevance_ratio NUMERIC(6,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN news_sentiment_filtered NUMERIC(6,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN sector_news_sentiment NUMERIC(6,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- Phase 7 대안 데이터
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN google_trend_score NUMERIC(6,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN google_trend_momentum NUMERIC(10,6); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN community_post_volume NUMERIC(10,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN community_comment_volume NUMERIC(10,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN community_engagement_ratio NUMERIC(8,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE feature_store ADD COLUMN alternative_activity_index NUMERIC(8,4); EXCEPTION WHEN duplicate_column THEN NULL; END $$;


CREATE TABLE IF NOT EXISTS ml_model (
    id                SERIAL PRIMARY KEY,
    model_name        VARCHAR(100)  NOT NULL,
    model_type        VARCHAR(30)   NOT NULL,
    algorithm         VARCHAR(30)   NOT NULL,
    market            VARCHAR(10)   NOT NULL,
    target_column     VARCHAR(50)   NOT NULL,
    hyperparameters   VARCHAR(2000),
    feature_columns   VARCHAR(2000),
    train_start_date  DATE,
    train_end_date    DATE,
    train_sample_count INTEGER,
    accuracy          NUMERIC(6,4),
    precision_score   NUMERIC(6,4),
    recall            NUMERIC(6,4),
    f1_score          NUMERIC(6,4),
    auc_roc           NUMERIC(6,4),
    mse               NUMERIC(15,6),
    rmse              NUMERIC(15,6),
    mae               NUMERIC(15,6),
    r2_score          NUMERIC(8,6),
    model_path        VARCHAR(500),
    is_active         BOOLEAN DEFAULT FALSE,
    version           INTEGER DEFAULT 1,
    created_at        TIMESTAMP DEFAULT NOW(),
    updated_at        TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_ml_model UNIQUE (model_name, version)
);
CREATE INDEX IF NOT EXISTS idx_ml_model_active ON ml_model (market, model_type, is_active);


CREATE TABLE IF NOT EXISTS ml_training_log (
    id                      SERIAL PRIMARY KEY,
    model_id                INTEGER REFERENCES ml_model(id) ON DELETE SET NULL,
    algorithm               VARCHAR(30)  NOT NULL,
    model_type              VARCHAR(30)  NOT NULL,
    market                  VARCHAR(10)  NOT NULL,
    target_column           VARCHAR(50)  NOT NULL,
    train_start_date        DATE,
    train_end_date          DATE,
    val_start_date          DATE,
    val_end_date            DATE,
    train_samples           INTEGER,
    val_samples             INTEGER,
    feature_count           INTEGER,
    status                  VARCHAR(20)  NOT NULL DEFAULT 'running',
    metrics_json            VARCHAR(3000),
    feature_importance_json VARCHAR(5000),
    hyperparameters_json    VARCHAR(2000),
    optuna_trials           INTEGER,
    best_trial_value        NUMERIC(10,6),
    started_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at             TIMESTAMP,
    error_message           VARCHAR(1000)
);
CREATE INDEX IF NOT EXISTS idx_training_log_model  ON ml_training_log (model_id);
CREATE INDEX IF NOT EXISTS idx_training_log_status ON ml_training_log (status, started_at);


CREATE TABLE IF NOT EXISTS ml_prediction (
    id              SERIAL PRIMARY KEY,
    model_id        INTEGER NOT NULL REFERENCES ml_model(id) ON DELETE CASCADE,
    market          VARCHAR(10)  NOT NULL,
    code            VARCHAR(20)  NOT NULL,
    prediction_date DATE         NOT NULL,
    target_date     DATE         NOT NULL,
    predicted_class INTEGER,
    probability_up  NUMERIC(6,4),
    probability_down NUMERIC(6,4),
    predicted_return NUMERIC(10,6),
    signal          VARCHAR(10),
    confidence      NUMERIC(6,4),
    created_at      TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_ml_prediction UNIQUE (model_id, market, code, prediction_date)
);
CREATE INDEX IF NOT EXISTS idx_prediction_lookup ON ml_prediction (market, code, prediction_date);
CREATE INDEX IF NOT EXISTS idx_prediction_signal ON ml_prediction (signal, prediction_date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 8. 스케줄러
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CREATE TABLE IF NOT EXISTS schedule_job (
    id          SERIAL PRIMARY KEY,
    job_name    VARCHAR(50)   NOT NULL,
    market      VARCHAR(10)   NOT NULL,
    sector      VARCHAR(50),
    cron_expr   VARCHAR(100)  NOT NULL,
    days_back   INTEGER       NOT NULL DEFAULT 7,
    enabled     BOOLEAN       NOT NULL DEFAULT TRUE,
    description VARCHAR(200),
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_schedule_job_name UNIQUE (job_name)
);

CREATE TABLE IF NOT EXISTS job_step (
    id          SERIAL PRIMARY KEY,
    job_id      INTEGER NOT NULL REFERENCES schedule_job(id) ON DELETE CASCADE,
    step_type   VARCHAR(30)  NOT NULL,
    step_order  INTEGER      NOT NULL,
    enabled     BOOLEAN      NOT NULL DEFAULT TRUE,
    config      TEXT,
    CONSTRAINT uq_job_step_type UNIQUE (job_id, step_type)
);

CREATE TABLE IF NOT EXISTS schedule_log (
    id             SERIAL PRIMARY KEY,
    job_id         INTEGER NOT NULL REFERENCES schedule_job(id) ON DELETE CASCADE,
    status         VARCHAR(20) NOT NULL DEFAULT 'running',
    started_at     TIMESTAMP   NOT NULL DEFAULT NOW(),
    finished_at    TIMESTAMP,
    total_codes    INTEGER DEFAULT 0,
    success_count  INTEGER DEFAULT 0,
    failed_count   INTEGER DEFAULT 0,
    db_saved_count INTEGER DEFAULT 0,
    trigger_by     VARCHAR(20) NOT NULL DEFAULT 'manual',
    message        VARCHAR(500)
);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 9. 백테스트 (Phase M5) ★ 신규
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CREATE TABLE IF NOT EXISTS backtest_run (
    id                SERIAL PRIMARY KEY,
    name              VARCHAR(200)   NOT NULL,
    market            VARCHAR(10)    NOT NULL,
    strategy          VARCHAR(50)    NOT NULL,
    start_date        DATE           NOT NULL,
    end_date          DATE           NOT NULL,
    config_json       VARCHAR(5000),
    initial_capital   NUMERIC(15,2)  NOT NULL DEFAULT 10000000,
    transaction_fee   NUMERIC(8,6)   DEFAULT 0.00015,
    tax_rate          NUMERIC(8,6)   DEFAULT 0.0023,
    codes_json        VARCHAR(5000),
    total_return      NUMERIC(10,6),
    annualized_return NUMERIC(10,6),
    sharpe_ratio      NUMERIC(10,6),
    sortino_ratio     NUMERIC(10,6),
    max_drawdown      NUMERIC(10,6),
    calmar_ratio      NUMERIC(10,6),
    win_rate          NUMERIC(8,6),
    profit_factor     NUMERIC(10,4),
    total_trades      INTEGER,
    benchmark_return  NUMERIC(10,6),
    alpha             NUMERIC(10,6),
    race_group        VARCHAR(36),
    status            VARCHAR(20)    DEFAULT 'running',
    error_message     VARCHAR(1000),
    started_at        TIMESTAMP DEFAULT NOW(),
    finished_at       TIMESTAMP,
    created_at        TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_backtest_run_market     ON backtest_run (market, status);
CREATE INDEX IF NOT EXISTS idx_backtest_run_date       ON backtest_run (start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_backtest_run_race_group ON backtest_run (race_group);

-- race_group 컬럼 (레이스 기능 추가 시)
DO $$ BEGIN
    ALTER TABLE backtest_run ADD COLUMN race_group VARCHAR(36);
EXCEPTION WHEN duplicate_column THEN NULL; END $$;


CREATE TABLE IF NOT EXISTS backtest_trade (
    id                   SERIAL PRIMARY KEY,
    run_id               INTEGER NOT NULL REFERENCES backtest_run(id) ON DELETE CASCADE,
    market               VARCHAR(10)   NOT NULL,
    code                 VARCHAR(20)   NOT NULL,
    trade_date           DATE          NOT NULL,
    action               VARCHAR(10)   NOT NULL,
    price                NUMERIC(15,2) NOT NULL,
    shares               INTEGER       NOT NULL,
    amount               NUMERIC(15,2) NOT NULL,
    fee                  NUMERIC(15,4),
    tax                  NUMERIC(15,4),
    signal_source        VARCHAR(100),
    signal_confidence    NUMERIC(6,4),
    probability_up       NUMERIC(6,4),
    cash_after           NUMERIC(15,2),
    portfolio_value_after NUMERIC(15,2),
    created_at           TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_backtest_trade_run  ON backtest_trade (run_id);
CREATE INDEX IF NOT EXISTS idx_backtest_trade_code ON backtest_trade (run_id, code, trade_date);


CREATE TABLE IF NOT EXISTS backtest_daily (
    id               SERIAL PRIMARY KEY,
    run_id           INTEGER NOT NULL REFERENCES backtest_run(id) ON DELETE CASCADE,
    date             DATE    NOT NULL,
    portfolio_value  NUMERIC(15,2) NOT NULL,
    cash             NUMERIC(15,2),
    positions_value  NUMERIC(15,2),
    daily_return     NUMERIC(10,6),
    cumulative_return NUMERIC(10,6),
    drawdown         NUMERIC(10,6),
    benchmark_value  NUMERIC(15,2),
    benchmark_return NUMERIC(10,6),
    positions_json   VARCHAR(5000),
    created_at       TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_backtest_daily UNIQUE (run_id, date)
);
CREATE INDEX IF NOT EXISTS idx_backtest_daily_run ON backtest_daily (run_id, date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 완료
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 총 20 테이블, 전체 Phase 1~M5 반영
-- CREATE IF NOT EXISTS → 신규 환경에서 안전하게 실행
-- DO $$ ALTER ... EXCEPTION → 기존 환경에서 중복 컬럼 무시
