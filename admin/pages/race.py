"""레이스 페이지 — 모델 레이스 + 종목 레이스"""

import json
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from admin.api.client import admin_client
from admin.pages.components import inject_custom_css, pct

# ── 이모지 매핑 ──────────────────────────────────────
MODEL_EMOJIS = {
    "random_forest": "\U0001f98a",
    "xgboost": "\U0001f680",
    "lightgbm": "\u26a1",
    "lstm": "\U0001f9e0",
    "transformer": "\U0001f916",
    "dqn": "\U0001f3ae",
    "ppo": "\U0001f3af",
}

STOCK_EMOJIS = ["\U0001f3ce\ufe0f", "\U0001f699", "\U0001f695", "\U0001f6fb", "\U0001f3cd\ufe0f", "\U0001f6b2", "\U0001f6f4"]

RACE_COLORS = {
    "random_forest": "#2e7d32",
    "xgboost": "#1565c0",
    "lightgbm": "#6a1b9a",
    "lstm": "#e65100",
    "transformer": "#00695c",
    "dqn": "#c62828",
    "ppo": "#4527a0",
}

STOCK_COLORS = ["#1565c0", "#2e7d32", "#e65100", "#6a1b9a", "#00695c", "#c62828", "#4527a0"]

PERIOD_PRESETS = {
    "1주일": 7,
    "30일": 30,
    "60일": 60,
    "100일": 100,
    "1년": 365,
}


def render():
    inject_custom_css()
    st.header("레이스")

    tab_model, tab_stock = st.tabs(["모델 레이스", "종목 레이스"])

    with tab_model:
        _render_model_race()

    with tab_stock:
        _render_stock_race()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 1 — 모델 레이스
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_model_race():
    # 활성 모델 로드
    try:
        all_models = admin_client.get_ml_models()
        active_models = [m for m in all_models if m.get("is_active")]
    except Exception:
        active_models = []

    if not active_models:
        st.info("활성 모델이 없습니다. ML 모델을 먼저 학습하세요.")
        return

    # 모델 선택 옵션
    model_options = {}
    for m in active_models:
        algo = m.get("algorithm", "?")
        label = f"[{m['id']}] {m['model_name']} ({algo})"
        model_options[label] = m["id"]

    with st.container(border=True):
        st.markdown("**레이스 설정**")
        c1, c2 = st.columns([1, 1])
        market = c1.selectbox("마켓", ["KOSPI", "KOSDAQ", "NYSE", "NASDAQ"], key="race_model_market")
        period_label = c2.selectbox("기간", list(PERIOD_PRESETS.keys()), index=1, key="race_model_period")

        selected_labels = st.multiselect(
            "참가 모델",
            options=list(model_options.keys()),
            default=list(model_options.keys()),
            key="race_model_select",
        )

        codes_input = st.text_input(
            "종목 코드 (쉼표 구분, 비우면 전종목)",
            placeholder="005930, 000660, 042700",
            key="race_model_codes",
        )

        c3, c4 = st.columns([1, 1])
        initial_capital = c3.number_input(
            "초기 자본금", value=10_000_000, step=1_000_000,
            min_value=1_000_000, key="race_model_capital",
        )
        auto_backfill = c4.checkbox(
            "빠진 날짜 소급 예측", value=True, key="race_auto_backfill",
            help="예측을 안 돌린 날짜가 있으면 자동으로 소급 예측을 생성합니다",
        )

        run_btn = st.button("레이스 시작", key="run_model_race", type="primary")

    if run_btn:
        if not selected_labels:
            st.error("모델을 1개 이상 선택하세요.")
            return

        selected_ids = [model_options[lbl] for lbl in selected_labels]
        period_days = PERIOD_PRESETS[period_label]
        codes = [c.strip() for c in codes_input.split(",") if c.strip()] if codes_input.strip() else None

        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=period_days)

        data = {
            "market": market,
            "model_ids": selected_ids,
            "codes": codes,
            "start_date": start_dt.strftime("%Y-%m-%d"),
            "end_date": end_dt.strftime("%Y-%m-%d"),
            "initial_capital": initial_capital,
            "auto_backfill": auto_backfill,
        }

        with st.spinner("모델 레이스 실행 중..."):
            try:
                result = admin_client.run_model_race(data)
            except Exception as e:
                st.error(f"레이스 실패: {e}")
                return

        st.session_state["model_race_result"] = result

    # 결과 렌더링
    result = st.session_state.get("model_race_result")
    if result:
        _render_model_race_result(result)


def _render_model_race_result(result: dict):
    summary = result.get("summary", {})
    participants = result.get("participants", [])

    # 서머리 카드
    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.metric("참가 모델", summary.get("total_models", 0))
    sc2.metric("성공", summary.get("success_count", 0))
    sc3.metric("1위", f"{summary.get('best_model', '-')} ({pct(summary.get('best_return'))})")
    sc4.metric("꼴찌", f"{summary.get('worst_model', '-')} ({pct(summary.get('worst_return'))})")

    # 소급 예측 통계
    total_filled = sum(
        (p.get("backfill_stats") or {}).get("filled", 0) for p in participants
    )
    if total_filled > 0:
        st.caption(f"소급 예측 {total_filled}건 자동 생성")

    st.markdown("---")

    # 레이싱 바 애니메이션
    st.subheader("레이싱")
    _render_racing_bars(participants, race_type="model")

    st.markdown("---")

    # 에쿼티 커브 차트 (애니메이션)
    st.subheader("에쿼티 커브")
    chart_data = []
    for p in participants:
        if p.get("status") != "success" or not p.get("equity_curve"):
            continue
        algo = p.get("algorithm", "unknown")
        color = RACE_COLORS.get(algo, "#888")
        emoji = MODEL_EMOJIS.get(algo, "")
        dates = [e["date"] for e in p["equity_curve"]]
        returns = [(e.get("cumulative_return") or 0) * 100 for e in p["equity_curve"]]
        chart_data.append({
            "name": f"{emoji} {p.get('model_name', algo)}",
            "dates": dates,
            "returns": returns,
            "color": color,
            "emoji": emoji,
        })
    _render_animated_equity_chart(chart_data, key="model")

    st.markdown("---")

    # 성과 지표 테이블
    st.subheader("성과 지표")
    _render_model_metrics_table(participants)


def _render_model_metrics_table(participants: list[dict]):
    rows = []
    for p in participants:
        metrics = p.get("metrics", {})
        algo = p.get("algorithm", "-")
        emoji = MODEL_EMOJIS.get(algo, "")
        rows.append({
            "": emoji,
            "모델": p.get("model_name", "-"),
            "알고리즘": algo,
            "상태": p.get("status", "-"),
            "수익률": pct(metrics.get("total_return")),
            "샤프": f"{metrics.get('sharpe_ratio', 0) or 0:.2f}",
            "MDD": pct(metrics.get("max_drawdown")),
            "승률": pct(metrics.get("win_rate")),
            "거래수": metrics.get("total_trades", 0) or 0,
        })
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 탭 2 — 종목 레이스
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_stock_race():
    with st.container(border=True):
        st.markdown("**레이스 설정**")
        c1, c2 = st.columns([1, 1])
        market = c1.selectbox("마켓", ["KOSPI", "KOSDAQ", "NYSE", "NASDAQ"], key="race_stock_market")
        period_label = c2.selectbox("기간", list(PERIOD_PRESETS.keys()), index=1, key="race_stock_period")

        codes_input = st.text_input(
            "종목 코드 (쉼표 구분)",
            placeholder="005930, 000660, 035720",
            key="race_stock_codes",
        )

        run_btn = st.button("레이스 시작", key="run_stock_race", type="primary")

    if run_btn:
        codes = [c.strip() for c in codes_input.split(",") if c.strip()]
        if len(codes) < 2:
            st.error("종목을 2개 이상 입력하세요.")
            return

        period_days = PERIOD_PRESETS[period_label]
        data = {
            "market": market,
            "codes": codes,
            "period_days": period_days,
        }

        with st.spinner("종목 레이스 실행 중..."):
            try:
                result = admin_client.run_stock_race(data)
            except Exception as e:
                st.error(f"레이스 실패: {e}")
                return

        st.session_state["stock_race_result"] = result

    result = st.session_state.get("stock_race_result")
    if result:
        _render_stock_race_result(result)


def _render_stock_race_result(result: dict):
    summary = result.get("summary", {})
    participants = result.get("participants", [])

    # 서머리 카드
    sc1, sc2, sc3 = st.columns(3)
    sc1.metric("참가 종목", summary.get("total_stocks", 0))
    sc2.metric("1위", f"{summary.get('best_stock', '-')} ({pct(summary.get('best_return'))})")
    sc3.metric("꼴찌", f"{summary.get('worst_stock', '-')} ({pct(summary.get('worst_return'))})")

    st.markdown("---")

    # 레이싱 바 애니메이션
    st.subheader("레이싱")
    _render_racing_bars(participants, race_type="stock")

    st.markdown("---")

    # 종가 수익률 차트 (애니메이션)
    st.subheader("종가 수익률")
    chart_data = []
    for i, p in enumerate(participants):
        if not p.get("equity_curve"):
            continue
        color = STOCK_COLORS[i % len(STOCK_COLORS)]
        emoji = STOCK_EMOJIS[i % len(STOCK_EMOJIS)]
        name = p.get("name") or p.get("code", "?")
        dates = [e["date"] for e in p["equity_curve"]]
        returns = [(e.get("cumulative_return") or 0) * 100 for e in p["equity_curve"]]
        chart_data.append({
            "name": f"{emoji} {name}",
            "dates": dates,
            "returns": returns,
            "color": color,
            "emoji": emoji,
        })
    _render_animated_equity_chart(chart_data, key="stock")

    st.markdown("---")

    # 최종 수익률 테이블
    st.subheader("최종 수익률")
    _render_stock_results_table(participants)


def _render_stock_results_table(participants: list[dict]):
    rows = []
    for i, p in enumerate(participants):
        emoji = STOCK_EMOJIS[i % len(STOCK_EMOJIS)]
        rows.append({
            "": emoji,
            "종목코드": p.get("code", "-"),
            "종목명": p.get("name", "-"),
            "수익률": pct(p.get("total_return")),
            "오류": p.get("error_message") or "",
        })
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 공통 — 레이싱 바 애니메이션 (JS)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_racing_bars(participants: list[dict], race_type: str = "model"):
    """수익률 기반 레이싱 바 — JS 애니메이션으로 0→최종값"""

    items = []
    for i, p in enumerate(participants):
        if race_type == "model":
            ret = (p.get("metrics") or {}).get("total_return")
            name = p.get("model_name") or p.get("algorithm", "?")
            algo = p.get("algorithm", "unknown")
            emoji = MODEL_EMOJIS.get(algo, "\U0001f3c3")
            color = RACE_COLORS.get(algo, "#888")
            failed = p.get("status") != "success"
        else:
            ret = p.get("total_return")
            name = p.get("name") or p.get("code", "?")
            emoji = STOCK_EMOJIS[i % len(STOCK_EMOJIS)]
            color = STOCK_COLORS[i % len(STOCK_COLORS)]
            failed = bool(p.get("error_message"))

        items.append({
            "name": name,
            "ret": float(ret) if ret is not None else 0,
            "emoji": emoji,
            "color": color,
            "failed": failed,
        })

    if not items:
        st.info("레이스 결과가 없습니다.")
        return

    items_json = json.dumps(items, ensure_ascii=False)
    row_h = 46
    total_h = len(items) * row_h + 30

    html = f"""
    <div id="raceBarContainer" style="padding:8px 0;font-family:-apple-system,BlinkMacSystemFont,sans-serif;"></div>
    <script>
    (function() {{
      const items = {items_json};
      const container = document.getElementById('raceBarContainer');

      // 정렬: 수익률 내림차순
      items.sort((a, b) => b.ret - a.ret);

      // 정규화: 0 기준 비례
      const absMax = Math.max(...items.map(d => Math.abs(d.ret)), 0.001);

      // 행 생성
      const rows = [];
      items.forEach((d, idx) => {{
        const row = document.createElement('div');
        row.style.cssText = 'margin:5px 0;display:flex;align-items:center;gap:8px;opacity:' + (d.failed ? '0.4' : '1');

        const label = document.createElement('span');
        label.style.cssText = 'width:140px;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex-shrink:0;';
        label.textContent = d.name;

        const track = document.createElement('div');
        track.style.cssText = 'flex:1;background:#f0f0f0;border-radius:8px;height:36px;position:relative;overflow:hidden;';

        const bar = document.createElement('div');
        bar.style.cssText = 'width:0%;height:100%;border-radius:8px;background:linear-gradient(90deg,' + d.color + 'cc,' + d.color + ');transition:none;';

        const emojiSpan = document.createElement('span');
        emojiSpan.style.cssText = 'position:absolute;left:0%;top:50%;font-size:24px;transform:translate(-50%,-50%) scaleX(-1);filter:drop-shadow(0 1px 2px rgba(0,0,0,0.3));transition:none;';
        emojiSpan.textContent = d.emoji;

        track.appendChild(bar);
        track.appendChild(emojiSpan);

        const retLabel = document.createElement('span');
        retLabel.style.cssText = 'width:80px;text-align:right;font-size:13px;font-weight:600;flex-shrink:0;';
        retLabel.textContent = d.failed ? 'FAIL' : (d.ret * 100).toFixed(1) + '%';

        row.appendChild(label);
        row.appendChild(track);
        row.appendChild(retLabel);
        container.appendChild(row);

        // 목표 퍼센트
        let targetPct;
        if (d.failed) {{
          targetPct = 0;
        }} else if (d.ret >= 0) {{
          targetPct = 8 + (d.ret / absMax) * 87;
        }} else {{
          targetPct = Math.max(3, 8 - (Math.abs(d.ret) / absMax) * 5);
        }}

        rows.push({{ bar, emojiSpan, targetPct, delay: idx * 150 }});
      }});

      // 애니메이션: 3초에 걸쳐 0→목표
      const DURATION = 3000;
      const start = performance.now();

      function tick(now) {{
        const elapsed = now - start;
        let allDone = true;

        rows.forEach(r => {{
          const t = Math.max(0, Math.min(1, (elapsed - r.delay) / DURATION));
          // ease-out cubic
          const ease = 1 - Math.pow(1 - t, 3);
          const pct = ease * r.targetPct;
          r.bar.style.width = pct.toFixed(1) + '%';
          r.emojiSpan.style.left = pct.toFixed(1) + '%';
          if (t < 1) allDone = false;
        }});

        if (!allDone) requestAnimationFrame(tick);
      }}

      requestAnimationFrame(tick);
    }})();
    </script>
    """
    components.html(html, height=total_h)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 공통 — 에쿼티 커브 애니메이션 (Canvas)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_animated_equity_chart(chart_data: list[dict], key: str = ""):
    """좌→우 7초 라인 드로잉 애니메이션 + 이모지 헤드"""

    if not chart_data:
        st.info("차트 데이터가 없습니다.")
        return

    chart_json = json.dumps(chart_data, ensure_ascii=False)
    uid = f"ec_{key}_{id(chart_data)}"

    html = f"""
    <div style="position:relative;width:100%;font-family:-apple-system,BlinkMacSystemFont,sans-serif;">
      <canvas id="{uid}" style="width:100%;height:400px;display:block;"></canvas>
      <div id="{uid}_leg" style="display:flex;flex-wrap:wrap;gap:14px;justify-content:center;padding:6px 0;font-size:12px;"></div>
    </div>
    <script>
    (function() {{
      const data = {chart_json};
      const canvas = document.getElementById('{uid}');
      const ctx = canvas.getContext('2d');
      const legendDiv = document.getElementById('{uid}_leg');

      // HiDPI
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      ctx.scale(dpr, dpr);
      const W = rect.width, H = rect.height;

      const pad = {{top: 24, right: 60, bottom: 40, left: 58}};
      const cw = W - pad.left - pad.right;
      const ch = H - pad.top - pad.bottom;

      // 전체 날짜 및 값 범위
      let allDates = [];
      let minY = 0, maxY = 0;
      data.forEach(d => {{
        if (d.dates.length > allDates.length) allDates = d.dates;
        d.returns.forEach(v => {{ if (v < minY) minY = v; if (v > maxY) maxY = v; }});
      }});
      const yPad = Math.max(Math.abs(maxY - minY) * 0.12, 0.5);
      minY -= yPad; maxY += yPad;
      const totalPts = allDates.length;
      if (totalPts < 2) return;

      // 범례
      data.forEach(d => {{
        const el = document.createElement('span');
        el.innerHTML = '<span style="display:inline-block;width:14px;height:3px;background:' + d.color + ';margin-right:4px;vertical-align:middle;border-radius:2px;"></span>' + d.name;
        legendDiv.appendChild(el);
      }});

      function xPos(i) {{ return pad.left + (i / (totalPts - 1)) * cw; }}
      function yPos(v) {{ return pad.top + (1 - (v - minY) / (maxY - minY)) * ch; }}

      // 그리드를 오프스크린 캔버스에 미리 그려놓기
      const gridCanvas = document.createElement('canvas');
      gridCanvas.width = canvas.width;
      gridCanvas.height = canvas.height;
      const gctx = gridCanvas.getContext('2d');
      gctx.scale(dpr, dpr);

      // 그리드 그리기
      gctx.strokeStyle = '#e8e8e8'; gctx.lineWidth = 0.5;
      const yTicks = 5;
      for (let i = 0; i <= yTicks; i++) {{
        const v = minY + (maxY - minY) * i / yTicks;
        const y = yPos(v);
        gctx.beginPath(); gctx.moveTo(pad.left, y); gctx.lineTo(W - pad.right, y); gctx.stroke();
        gctx.fillStyle = '#888'; gctx.font = '11px sans-serif'; gctx.textAlign = 'right';
        gctx.fillText(v.toFixed(1) + '%', pad.left - 8, y + 4);
      }}

      // 0% 기준선
      if (minY <= 0 && maxY >= 0) {{
        gctx.strokeStyle = '#aaa'; gctx.lineWidth = 1; gctx.setLineDash([5,3]);
        const y0 = yPos(0);
        gctx.beginPath(); gctx.moveTo(pad.left, y0); gctx.lineTo(W - pad.right, y0); gctx.stroke();
        gctx.setLineDash([]);
      }}

      // X축 라벨
      const labelCount = Math.min(7, totalPts);
      gctx.fillStyle = '#888'; gctx.font = '10px sans-serif'; gctx.textAlign = 'center';
      for (let i = 0; i < labelCount; i++) {{
        const idx = Math.round(i * (totalPts - 1) / (labelCount - 1));
        gctx.fillText(allDates[idx] ? allDates[idx].slice(5) : '', xPos(idx), H - pad.bottom + 18);
      }}

      // 차트 영역 테두리
      gctx.strokeStyle = '#ccc'; gctx.lineWidth = 1;
      gctx.strokeRect(pad.left, pad.top, cw, ch);

      // 애니메이션
      const DURATION = 7000;
      let startTime = null;

      function animate(ts) {{
        if (!startTime) startTime = ts;
        const elapsed = ts - startTime;
        const progress = Math.min(elapsed / DURATION, 1);
        // ease-out quad — 부드러운 감속
        const ease = 1 - Math.pow(1 - progress, 2.5);
        // 실수 인덱스 (소수점까지) — 보간으로 부드럽게
        const drawIdx = ease * (totalPts - 1);

        // 그리드 복사
        ctx.clearRect(0, 0, W, H);
        ctx.drawImage(gridCanvas, 0, 0, canvas.width, canvas.height, 0, 0, W, H);

        data.forEach(d => {{
          const maxI = Math.min(Math.floor(drawIdx), d.returns.length - 1);
          if (maxI < 1) return;

          // 라인 그리기
          ctx.strokeStyle = d.color;
          ctx.lineWidth = 2.5;
          ctx.lineJoin = 'round';
          ctx.lineCap = 'round';
          ctx.beginPath();
          ctx.moveTo(xPos(0), yPos(d.returns[0]));
          for (let i = 1; i <= maxI; i++) {{
            ctx.lineTo(xPos(i), yPos(d.returns[i]));
          }}

          // 마지막 구간 보간 (부드러운 이동)
          const frac = drawIdx - maxI;
          if (frac > 0 && maxI + 1 < d.returns.length) {{
            const interpY = d.returns[maxI] + (d.returns[maxI + 1] - d.returns[maxI]) * frac;
            ctx.lineTo(xPos(drawIdx), yPos(interpY));
          }}
          ctx.stroke();

          // 선두: 이모지 헤드
          const headIdx = Math.min(drawIdx, d.returns.length - 1);
          const headI = Math.floor(headIdx);
          const headFrac = headIdx - headI;
          let headVal = d.returns[headI];
          if (headFrac > 0 && headI + 1 < d.returns.length) {{
            headVal = d.returns[headI] + (d.returns[headI + 1] - d.returns[headI]) * headFrac;
          }}
          const hx = xPos(headIdx);
          const hy = yPos(headVal);

          // 이모지
          ctx.font = '18px serif';
          ctx.textAlign = 'center';
          ctx.fillText(d.emoji || '', hx, hy - 12);

          // 값 라벨
          if (progress > 0.15) {{
            ctx.fillStyle = d.color;
            ctx.font = 'bold 11px sans-serif';
            ctx.textAlign = 'left';
            const label = headVal.toFixed(1) + '%';
            ctx.fillText(label, hx + 14, hy + 4);
          }}
        }});

        if (progress < 1) requestAnimationFrame(animate);
      }}

      requestAnimationFrame(animate);
    }})();
    </script>
    """
    components.html(html, height=450)
