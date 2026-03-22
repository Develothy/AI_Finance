"""공통 UI 컴포넌트 — 어드민 페이지 전체에서 재사용"""

import streamlit as st

# ── 알고리즘 컬러 매핑 ──────────────────────────────────
ALGO_COLORS = {
    "random_forest": ("#2e7d32", "#e8f5e9", "RF"),
    "xgboost": ("#1565c0", "#e3f2fd", "XGB"),
    "lightgbm": ("#6a1b9a", "#f3e5f5", "LGBM"),
}

ALGO_LABELS = {
    "random_forest": "Random Forest",
    "xgboost": "XGBoost",
    "lightgbm": "LightGBM",
}

SIGNAL_COLORS = {
    "BUY": ("#4CAF50", "#e8f5e9", "BUY"),
    "SELL": ("#f44336", "#ffebee", "SELL"),
    "HOLD": ("#FF9800", "#fff3e0", "HOLD"),
}


# ── CSS 주입 ────────────────────────────────────────────
def inject_custom_css():
    st.markdown("""
    <style>
    /* 뱃지 공통 */
    .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 0.78rem;
        font-weight: 600;
        letter-spacing: 0.3px;
    }
    /* 메트릭 카드 */
    .metric-card {
        text-align: center;
        padding: 8px 4px;
    }
    .metric-card .value {
        font-size: 1.5rem;
        font-weight: 700;
        line-height: 1.2;
    }
    .metric-card .label {
        font-size: 0.8rem;
        color: #888;
        margin-top: 2px;
    }
    .metric-card .delta {
        font-size: 0.75rem;
        margin-top: 2px;
    }
    /* 상태 점 */
    .status-dot {
        display: inline-block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        margin-right: 6px;
        vertical-align: middle;
    }
    /* 탭 내 컨테이너 여백 */
    div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlock"] {
        gap: 0.5rem;
    }
    /* ── 파이프라인 플로우 ── */
    .pipeline-flow {
        display: flex;
        flex-wrap: wrap;
        gap: 4px;
        align-items: center;
        padding: 10px 0;
    }
    .pipeline-node {
        border-radius: 8px;
        padding: 8px 10px;
        text-align: center;
        font-size: 0.78rem;
        min-width: 68px;
        border: 2px solid;
        background: rgba(255,255,255,0.03);
        line-height: 1.4;
    }
    .pipeline-node .node-emoji { font-size: 1.1rem; }
    .pipeline-node .node-name { font-weight: 600; }
    .pipeline-node .node-meta {
        font-size: 0.7rem;
        color: #aaa;
        margin-top: 2px;
    }
    .pipeline-arrow {
        color: #555;
        font-size: 1rem;
        padding: 0 1px;
    }
    /* 상태별 노드 */
    .pipeline-node.st-success  { border-color: #4CAF50; }
    .pipeline-node.st-completed { border-color: #4CAF50; }
    .pipeline-node.st-failed   { border-color: #f44336; }
    .pipeline-node.st-error    { border-color: #f44336; }
    .pipeline-node.st-running  { border-color: #1565c0; }
    .pipeline-node.st-partial  { border-color: #FF9800; }
    .pipeline-node.st-pending  { border-color: #555; opacity: 0.6; }
    .pipeline-node.st-skipped  { border-color: #444; opacity: 0.5; }
    /* ── 로그 카드 헤더 ── */
    .log-card-header {
        display: flex;
        align-items: baseline;
        gap: 10px;
        flex-wrap: wrap;
    }
    .log-card-header .log-title {
        font-size: 1.05rem;
        font-weight: 600;
    }
    .log-card-header .log-sub {
        font-size: 0.82rem;
        color: #999;
    }
    .log-card-header .log-trigger {
        font-size: 0.72rem;
        padding: 1px 8px;
        border-radius: 10px;
        background: #262730;
        color: #aaa;
    }
    </style>
    """, unsafe_allow_html=True)


# ── 뱃지 함수들 ────────────────────────────────────────

def algo_badge(algorithm: str) -> str:
    fg, bg, short = ALGO_COLORS.get(algorithm, ("#666", "#eee", algorithm))
    return f'<span class="badge" style="color:{fg};background:{bg};">{short}</span>'


def status_badge(is_active: bool) -> str:
    if is_active:
        return '<span class="badge" style="color:#2e7d32;background:#e8f5e9;">Active</span>'
    return '<span class="badge" style="color:#888;background:#eee;">Inactive</span>'


def signal_badge(signal: str) -> str:
    fg, bg, label = SIGNAL_COLORS.get(signal, ("#666", "#eee", signal))
    return f'<span class="badge" style="color:{fg};background:{bg};">{label}</span>'


def status_dot(enabled: bool) -> str:
    color = "#4CAF50" if enabled else "#999"
    label = "활성" if enabled else "비활성"
    return f'<span class="status-dot" style="background:{color};"></span>{label}'


# ── 메트릭 카드 (HTML) ─────────────────────────────────

def metric_card(label: str, value: str, delta: str | None = None, color: str | None = None) -> str:
    val_style = f"color:{color};" if color else ""
    delta_html = ""
    if delta is not None:
        delta_html = f'<div class="delta">{delta}</div>'
    return (
        f'<div class="metric-card">'
        f'<div class="value" style="{val_style}">{value}</div>'
        f'<div class="label">{label}</div>'
        f'{delta_html}'
        f'</div>'
    )


# ── 포맷 헬퍼 ──────────────────────────────────────────

def pct(val, digits: int = 1) -> str:
    if val is None:
        return "-"
    return f"{val * 100:.{digits}f}%"


def fmt(val, digits: int = 4) -> str:
    if val is None:
        return "-"
    return f"{val:.{digits}f}"
