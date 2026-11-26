"""
NYC 311 Service Requests & Weather - Decision Dashboard (v2)
DAT535 - Group 13

Design rules for this dashboard
-------------------------------
1. ONE time scope (All time / Month / Week / Day) in the sidebar drives every
   page. Every page repeats the active scope in a banner at the top.
   Anything that cannot follow the scope says so explicitly.
2. Plain language. No internal pipeline jargon on the screen.
3. Every number is computed from the Gold marts with the grain respected:
     demand mart  : one row per day x weather-grid-cell x borough x complaint type
     agency mart  : one row per day x borough x agency
     ZIP mart     : one row per ZIP x complaint type, whole period (NO date column)
4. No made-up data. If a mart is missing, the app says so and stops.

Run:  streamlit run app-v2.py
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# =============================================================================
# 1. PAGE SETUP
# =============================================================================
st.set_page_config(
    page_title="NYC 311 Decision Dashboard",
    page_icon="🗽",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .scope-banner {
        background: #eff6ff; border: 1px solid #bfdbfe; border-left: 6px solid #2563eb;
        border-radius: 8px; padding: 10px 16px; margin: 4px 0 14px 0; color: #1e3a8a;
        font-size: 0.95rem;
    }
    .scope-banner b { color: #1e40af; }
    .scope-warning {
        background: #fffbeb; border: 1px solid #fde68a; border-left: 6px solid #f59e0b;
        border-radius: 8px; padding: 10px 16px; margin: 4px 0 14px 0; color: #78350f;
        font-size: 0.95rem;
    }
    .takeaway {
        background: #f8fafc; border-left: 4px solid #64748b; border-radius: 6px;
        padding: 8px 14px; margin: 6px 0 14px 0; color: #334155; font-size: 0.93rem;
    }
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%) !important;
        border: 1px solid #334155 !important;
        border-radius: 10px !important;
        padding: 14px 18px !important;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2), 0 2px 4px -1px rgba(0, 0, 0, 0.1) !important;
    }
    div[data-testid="stMetricLabel"] {
        color: #94a3b8 !important;
    }
    div[data-testid="stMetricLabel"] > div,
    div[data-testid="stMetricLabel"] p,
    div[data-testid="stMetricLabel"] label {
        color: #94a3b8 !important;
        font-size: 0.82rem !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.5px !important;
    }
    div[data-testid="stMetricLabel"] svg {
        fill: #94a3b8 !important;
        color: #94a3b8 !important;
    }
    div[data-testid="stMetricValue"],
    div[data-testid="stMetricValue"] > div {
        color: #f8fafc !important;
        font-size: 1.85rem !important;
        font-weight: 700 !important;
    }
    div[data-testid="stMetricDelta"] {
        font-weight: 600 !important;
    }
    div[data-testid="stMetricDelta"] svg {
        fill: currentColor !important;
    }
    .stTabs [data-baseweb="tab"] { font-weight: 600; font-size: 1rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

PROJECT_ROOT = Path(__file__).resolve().parent
env_output_dir = os.environ.get("DAT535_OUTPUT_DIR")
DATA_DIR = Path(env_output_dir).resolve() if env_output_dir else (PROJECT_ROOT / "data")
MARTS_DIR = DATA_DIR / "gold" / "marts"
QUALITY_DIR = DATA_DIR / "metadata" / "quality"
REF_DIR = DATA_DIR / "reference"

BLUE, DARK_BLUE, ORANGE, RED, GREEN, GREY = (
    "#60a5fa", "#1d4ed8", "#f59e0b", "#dc2626", "#16a34a", "#94a3b8",
)

# Friendly labels (internal code  ->  what a reader understands)
SERVICE_AREA_LABELS = {
    "ACOUSTIC_DISTURBANCE": "Noise",
    "RESIDENTIAL_HABITABILITY": "Housing conditions",
    "STREET_TRAFFIC_INFRASTRUCTURE": "Streets & traffic",
    "SOLID_WASTE_SANITATION": "Sanitation & waste",
    "PARKS_FORESTRY": "Parks & trees",
    "CIVIC_REGULATION_CONSUMER": "Business & building rules",
    "PUBLIC_HEALTH_ENVIRONMENT": "Public health & environment",
    "WATER_INFRASTRUCTURE": "Water supply",
    "SEWER_STORMWATER": "Sewers & flooding",
    "SOCIAL_SERVICES_HOMELESSNESS": "Social services & homelessness",
    "OTHER_CIVIC_SERVICES": "Other services",
}
AGENCY_LABELS = {
    "NYPD": "Police (NYPD)",
    "HPD": "Housing Preservation (HPD)",
    "DSNY": "Sanitation (DSNY)",
    "DOT": "Transportation (DOT)",
    "DEP": "Environmental Protection (DEP)",
    "DOB": "Buildings (DOB)",
    "DPR": "Parks & Recreation (DPR)",
    "DOHMH": "Health (DOHMH)",
    "DHS": "Homeless Services (DHS)",
    "DCWP": "Consumer & Worker Protection (DCWP)",
    "TLC": "Taxi & Limousine (TLC)",
    "DOE": "Education (DOE)",
    "EDC": "Economic Development (EDC)",
    "OTI": "Technology (OTI)",
}
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
WEATHER_CONDITIONS = ["Dry (under 1 mm)", "Wet (1-15 mm)", "Storm (15 mm or more)"]
LONG_OPEN_DAYS = 7  # matches LONG_OPEN_BACKLOG_DAYS in src/gold/config.py
STORM_MM = 15.0     # matches STORM_PRECIP_THRESHOLD_MM in src/gold/config.py


# =============================================================================
# 2. DATA LOADING
# =============================================================================
def _find_mart(name: str) -> Optional[Path]:
    for candidate in (MARTS_DIR / f"{name}.parquet", MARTS_DIR / name):
        if candidate.exists():
            return candidate
    return None


@st.cache_data(show_spinner="Loading data...")
def load_marts():
    """Load the three Gold marts and the ZIP centroid reference table."""
    paths = {
        "demand": _find_mart("gold_daily_service_demand"),
        "perf": _find_mart("gold_agency_resolution_performance"),
        "zip": _find_mart("gold_zip_infrastructure_vulnerability"),
    }
    missing = [k for k, v in paths.items() if v is None]
    if missing:
        return None, None, None, None, missing

    demand = pd.read_parquet(paths["demand"])
    demand["date"] = pd.to_datetime(demand["date"])
    demand["borough"] = demand["borough"].astype(str).str.title()
    demand["service_area"] = (
        demand["canonical_service_domain"].map(SERVICE_AREA_LABELS)
        .fillna(demand["canonical_service_domain"].astype(str))
    )

    perf = pd.read_parquet(paths["perf"])
    perf["reporting_date"] = pd.to_datetime(perf["reporting_date"])
    perf["borough"] = perf["borough"].astype(str).str.title()

    zips = pd.read_parquet(paths["zip"])
    zips["borough"] = zips["borough"].astype(str).str.title()
    zips["incident_zip"] = zips["incident_zip"].astype(str)

    p_cent = REF_DIR / "nyc_zip_centroids.parquet"
    centroids = pd.read_parquet(p_cent) if p_cent.exists() else None
    if centroids is not None:
        centroids["zip5"] = centroids["zip5"].astype(str)
    return demand, perf, zips, centroids, []


@st.cache_data(show_spinner=False)
def load_quality():
    """Latest pipeline run report + the review ledgers."""
    report, taxonomy, resolutions = None, None, None
    if QUALITY_DIR.exists():
        reports = sorted(QUALITY_DIR.glob("silver_quality_*.json"))  # timestamp in name
        if reports:
            try:
                report = json.loads(reports[-1].read_text(encoding="utf-8"))
            except Exception:
                report = None
        p_tax = QUALITY_DIR / "unmapped_complaint_taxonomy_ledger.parquet"
        p_res = QUALITY_DIR / "unclassified_resolutions_ledger.parquet"
        if p_tax.exists():
            taxonomy = pd.read_parquet(p_tax)
        if p_res.exists():
            resolutions = pd.read_parquet(p_res)
    return report, taxonomy, resolutions


demand, perf, zips, centroids, missing_marts = load_marts()
if missing_marts:
    st.error(
        f"Gold data not found: {', '.join(missing_marts)}. "
        "Run `python run_gold.py --env dev` first, then reload this page."
    )
    st.stop()

DATA_MIN: date = demand["date"].min().date()
DATA_MAX: date = demand["date"].max().date()
_weather_dates = demand.loc[demand["total_precipitation_mm"].notna(), "date"]
WEATHER_MIN: Optional[date] = _weather_dates.min().date() if len(_weather_dates) else None
WEATHER_MAX: Optional[date] = _weather_dates.max().date() if len(_weather_dates) else None


# =============================================================================
# 3. SMALL HELPERS
# =============================================================================
def fmt_date(d: date, year: bool = True) -> str:
    return f"{d.day} {d.strftime('%b')}" + (f" {d.year}" if year else "")


def fmt_int(n) -> str:
    return f"{int(round(n)):,}"


def fmt_hours(h: Optional[float]) -> str:
    if h is None or pd.isna(h):
        return "n/a"
    return f"{h:.1f} hours" if h < 48 else f"{h / 24:.1f} days"


def safe_div(a, b, scale: float = 1.0) -> float:
    return float(a) / float(b) * scale if b else 0.0


def wmedian(values: pd.Series, weights: pd.Series) -> Optional[float]:
    """Weighted median (used to combine daily medians without being skewed by outliers)."""
    mask = values.notna() & weights.notna() & (weights > 0)
    if not mask.any():
        return None
    v, w = values[mask].to_numpy(), weights[mask].to_numpy(dtype=float)
    order = np.argsort(v)
    v, w = v[order], w[order]
    cum = np.cumsum(w)
    return float(v[np.searchsorted(cum, cum[-1] / 2.0)])


def delta_pct(cur, prev, label: Optional[str]) -> Optional[str]:
    if label is None or not prev:
        return None
    return f"{(cur - prev) / prev * 100:+.1f}% vs {label}"


def delta_pts(cur, prev, label: Optional[str]) -> Optional[str]:
    if label is None or prev is None:
        return None
    return f"{cur - prev:+.1f} pts vs {label}"


def style_fig(fig: go.Figure, height: int = 380) -> go.Figure:
    fig.update_layout(
        template="plotly_white",
        height=height,
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        font=dict(size=13),
    )
    return fig


def show(fig: go.Figure) -> None:
    st.plotly_chart(fig, width="stretch")


def takeaway(text: str) -> None:
    st.markdown(f'<div class="takeaway">💡 {text}</div>', unsafe_allow_html=True)


# =============================================================================
# 4. GLOBAL SCOPE (sidebar) - one scope for the whole dashboard
# =============================================================================
class Scope:
    def __init__(self, kind, start, end, label, prev_start, prev_end, prev_label, note):
        self.kind, self.start, self.end, self.label = kind, start, end, label
        self.prev_start, self.prev_end, self.prev_label = prev_start, prev_end, prev_label
        self.note = note
        self.n_days = (end - start).days + 1


def build_scope() -> Scope:
    st.sidebar.title("🗽 NYC 311")
    st.sidebar.caption("Service requests & weather | DAT535 Group 13")
    st.sidebar.subheader("Time scope")
    kind = st.sidebar.radio(
        "Show data for", ["All time", "Month", "Week", "Day"], index=1, horizontal=True,
        help="This choice applies to every page, except where a page says otherwise.",
    )

    note = ""
    if kind == "All time":
        start, end = DATA_MIN, DATA_MAX
        label = f"All time ({fmt_date(start)} to {fmt_date(end)})"
    elif kind == "Month":
        months = list(pd.period_range(DATA_MIN, DATA_MAX, freq="M"))[::-1]
        chosen = st.sidebar.selectbox("Month", months, format_func=lambda p: p.strftime("%B %Y"))
        start = max(chosen.start_time.date(), DATA_MIN)
        month_end = chosen.end_time.date()
        end = min(month_end, DATA_MAX)
        label = chosen.strftime("%B %Y")
        if end < month_end:
            note = f"Data ends {fmt_date(end)}, so this month is incomplete."
            label += f" (1-{fmt_date(end, year=False)})"
    elif kind == "Week":
        weeks, mon = [], DATA_MIN - timedelta(days=DATA_MIN.weekday())
        while mon <= DATA_MAX:
            weeks.append(mon)
            mon += timedelta(days=7)
        mon = st.sidebar.selectbox(
            "Week (Mon-Sun)", weeks[::-1],
            format_func=lambda m: f"{fmt_date(m, False)} - {fmt_date(m + timedelta(days=6))}",
        )
        start, end = max(mon, DATA_MIN), min(mon + timedelta(days=6), DATA_MAX)
        label = f"Week of {fmt_date(start, False)} - {fmt_date(end)}"
        if (end - start).days < 6:
            note = "This week is only partly covered by the data."
    else:
        day = st.sidebar.date_input("Day", value=DATA_MAX, min_value=DATA_MIN, max_value=DATA_MAX)
        start = end = day
        label = day.strftime("%A, %d %B %Y")

    # Comparison period: same position/length in the previous month / week / day
    prev_start = prev_end = prev_label = None
    if kind != "All time":
        span = end - start
        if kind == "Month":
            ps = (pd.Timestamp(start) - pd.DateOffset(months=1)).date().replace(day=1)
        elif kind == "Week":
            ps = start - timedelta(days=7)
        else:
            ps = start - timedelta(days=1)
        if ps >= DATA_MIN:
            prev_start, prev_end = ps, ps + span
            prev_label = (
                fmt_date(ps) if kind == "Day"
                else f"{fmt_date(ps, False)}-{fmt_date(prev_end)}"
            )

    st.sidebar.subheader("Boroughs")
    return Scope(kind, start, end, label, prev_start, prev_end, prev_label, note)


scope = build_scope()

all_boroughs = sorted(demand["borough"].unique().tolist())
selected_boroughs = st.sidebar.multiselect(
    "Include boroughs", all_boroughs, default=all_boroughs,
    help="'Unspecified' = requests with no usable borough in the source data.",
) or all_boroughs
borough_text = "All boroughs" if len(selected_boroughs) == len(all_boroughs) else ", ".join(selected_boroughs)

st.sidebar.markdown("---")
st.sidebar.caption(
    f"**Data coverage**  \n311 requests: {fmt_date(DATA_MIN)} to {fmt_date(DATA_MAX)}  \n"
    + (
        f"Weather: {fmt_date(WEATHER_MIN)} to {fmt_date(WEATHER_MAX)}"
        if WEATHER_MIN else "Weather: not available"
    )
)

# Borough-filtered base tables (time filtering is applied per page via `window`)
dem_b = demand[demand["borough"].isin(selected_boroughs)]
perf_b = perf[perf["borough"].isin(selected_boroughs)]


def window(df: pd.DataFrame, col: str, start: date, end: date) -> pd.DataFrame:
    return df[(df[col] >= pd.Timestamp(start)) & (df[col] <= pd.Timestamp(end))]


def banner(extra: str = "") -> None:
    """The scope statement shown at the top of every page."""
    st.markdown(
        f'<div class="scope-banner">📅 Showing <b>{scope.label}</b> &nbsp;|&nbsp; '
        f"🗺️ <b>{borough_text}</b>"
        + (f" &nbsp;|&nbsp; {extra}" if extra else "")
        + "</div>",
        unsafe_allow_html=True,
    )
    if scope.note:
        st.caption(f"⚠️ {scope.note}")


def warning_banner(text: str) -> None:
    st.markdown(f'<div class="scope-warning">⚠️ {text}</div>', unsafe_allow_html=True)


def trend_window() -> tuple[date, date, Optional[date], str]:
    """Date range for time-series charts. A single day gets 14 days of context."""
    if scope.kind == "Day":
        s = max(DATA_MIN, scope.end - timedelta(days=13))
        return s, scope.end, scope.end, "Trend shows the 14 days up to the selected day (selected day shaded)."
    return scope.start, scope.end, None, ""


def daily_totals(df: pd.DataFrame, date_col: str, cols: list[str]) -> pd.DataFrame:
    """Daily sums over the FULL data range (zero-filled), so rolling averages are correct at the edges."""
    daily = df.groupby(date_col)[cols].sum()
    full = pd.date_range(DATA_MIN, DATA_MAX, freq="D")
    return daily.reindex(full, fill_value=0).rename_axis("date")


# =============================================================================
# 5. PAGE HEADER
# =============================================================================
st.title("NYC 311: What residents report, and how the city responds")
tab_overview, tab_weather, tab_agency, tab_zip, tab_quality = st.tabs(
    [
        "📊 Overview",
        "🌧️ Weather & demand",
        "🏛️ Agency performance",
        "📍 Neighborhoods (ZIP)",
        "🛡️ Data quality",
    ]
)

# =============================================================================
# 6. OVERVIEW
# =============================================================================
with tab_overview:
    banner()
    cur = window(dem_b, "date", scope.start, scope.end)
    prv = (
        window(dem_b, "date", scope.prev_start, scope.prev_end)
        if scope.prev_start else None
    )

    if cur.empty:
        st.info("No 311 requests for this selection. Try a wider time scope or more boroughs.")
    else:
        calls = cur["complaint_count"].sum()
        incidents = cur["distinct_incidents_count"].sum()
        repeats = cur["duplicate_burst_calls_count"].sum()
        closed = cur["closed_complaint_count"].sum()
        repeat_share = safe_div(repeats, calls, 100)
        closed_share = safe_div(closed, calls, 100)

        p_calls = prv["complaint_count"].sum() if prv is not None else None
        p_inc = prv["distinct_incidents_count"].sum() if prv is not None else None
        p_rep_share = (
            safe_div(prv["duplicate_burst_calls_count"].sum(), p_calls, 100) if prv is not None else None
        )
        p_closed_share = (
            safe_div(prv["closed_complaint_count"].sum(), p_calls, 100) if prv is not None else None
        )

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric(
            "Requests received", fmt_int(calls),
            delta_pct(calls, p_calls, scope.prev_label), delta_color="off",
            help="Every 311 request logged in the period (one row = one request).",
        )
        k2.metric(
            "Distinct problems", fmt_int(incidents),
            delta_pct(incidents, p_inc, scope.prev_label), delta_color="off",
            help="Requests left after merging repeat reports of the same problem at the same place "
                 "close in time (e.g. five neighbours reporting one flooded street = 1 problem).",
        )
        k3.metric(
            "Repeat reports", f"{repeat_share:.1f}%",
            delta_pts(repeat_share, p_rep_share, scope.prev_label), delta_color="off",
            help=f"{fmt_int(repeats)} of the requests were repeat reports of a problem already "
                 "reported. Requests = distinct problems + repeat reports.",
        )
        k4.metric(
            "Resolved so far", f"{closed_share:.1f}%",
            delta_pts(closed_share, p_closed_share, scope.prev_label), delta_color="normal",
            help="Share of the requests received in this period that are now closed "
                 "(measured at the latest data date, so recent periods look lower).",
        )
        if scope.n_days > 1:
            k5.metric(
                "Average per day", fmt_int(calls / scope.n_days),
                delta_pct(calls / scope.n_days, (p_calls / ((scope.prev_end - scope.prev_start).days + 1)) if p_calls else None, scope.prev_label),
                delta_color="off", help="Requests received per calendar day in this period.",
            )
        else:
            top_type = cur.groupby("complaint_type")["complaint_count"].sum().idxmax()
            k5.metric("Most common complaint", top_type)

        # ---- Trend ---------------------------------------------------------
        t_start, t_end, shade, t_note = trend_window()
        series = daily_totals(dem_b, "date", ["complaint_count", "distinct_incidents_count"])
        series["avg7"] = series["complaint_count"].rolling(7, min_periods=1).mean()
        view = series.loc[pd.Timestamp(t_start):pd.Timestamp(t_end)].reset_index()

        st.markdown("#### How many requests come in each day?")
        fig = go.Figure()
        fig.add_bar(
            x=view["date"], y=view["complaint_count"], name="Requests received",
            marker_color=BLUE, opacity=0.7,
            hovertemplate="%{x|%a %d %b %Y}<br>%{y:,} requests<extra></extra>",
        )
        fig.add_scatter(
            x=view["date"], y=view["distinct_incidents_count"], name="Distinct problems",
            line=dict(color=DARK_BLUE, width=2),
            hovertemplate="%{x|%a %d %b %Y}<br>%{y:,} distinct problems<extra></extra>",
        )
        fig.add_scatter(
            x=view["date"], y=view["avg7"], name="7-day average (requests)",
            line=dict(color=ORANGE, width=3, dash="dash"),
            hovertemplate="%{x|%a %d %b %Y}<br>%{y:,.0f} avg<extra></extra>",
        )
        if shade:
            fig.add_vrect(
                x0=pd.Timestamp(shade) - pd.Timedelta(hours=12),
                x1=pd.Timestamp(shade) + pd.Timedelta(hours=12),
                fillcolor=ORANGE, opacity=0.2, line_width=0,
            )
        fig.update_yaxes(title="Requests per day", rangemode="tozero")
        show(style_fig(fig, 380))
        if t_note:
            st.caption(t_note)
        if scope.n_days > 1:
            peak = cur.groupby("date")["complaint_count"].sum()
            takeaway(
                f"Busiest day: <b>{peak.idxmax():%A %d %b}</b> with <b>{fmt_int(peak.max())}</b> requests "
                f"({peak.max() / peak.mean():.2f}x the daily average of {fmt_int(peak.mean())})."
            )

        # ---- Where & what ----------------------------------------------------
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### Which boroughs generate the requests?")
            by_b = cur.groupby("borough")["complaint_count"].sum().reset_index()
            f = px.pie(
                by_b, names="borough", values="complaint_count", hole=0.55,
                color_discrete_sequence=px.colors.qualitative.Safe,
            )
            f.update_traces(textinfo="label+percent", hovertemplate="%{label}: %{value:,} requests<extra></extra>")
            f.update_layout(showlegend=False)
            show(style_fig(f, 340))
            st.caption("Raw request counts (not adjusted for population).")
        with c2:
            st.markdown("#### Which services do residents ask for?")
            by_a = (
                cur.groupby("service_area")["complaint_count"].sum()
                .sort_values().reset_index()
            )
            by_a["share"] = by_a["complaint_count"] / by_a["complaint_count"].sum() * 100
            f = px.bar(
                by_a, x="complaint_count", y="service_area", orientation="h",
                text=by_a["share"].map(lambda s: f"{s:.0f}%"),
                color_discrete_sequence=[DARK_BLUE],
            )
            f.update_traces(hovertemplate="%{y}: %{x:,} requests<extra></extra>", textposition="outside")
            f.update_layout(xaxis_title="Requests", yaxis_title=None)
            show(style_fig(f, 340))

        c3, c4 = st.columns(2)
        with c3:
            st.markdown("#### Top 10 complaint types")
            top = (
                cur.groupby("complaint_type")["complaint_count"].sum()
                .nlargest(10).sort_values().reset_index()
            )
            f = px.bar(top, x="complaint_count", y="complaint_type", orientation="h",
                       color_discrete_sequence=[BLUE])
            f.update_traces(hovertemplate="%{y}: %{x:,} requests<extra></extra>")
            f.update_layout(xaxis_title="Requests", yaxis_title=None)
            show(style_fig(f, 380))
        with c4:
            st.markdown("#### Which day of the week is busiest?")
            if scope.n_days >= 7:
                per_day = cur.groupby("date")["complaint_count"].sum().reset_index()
                per_day["dow"] = per_day["date"].dt.dayofweek
                dow = per_day.groupby("dow")["complaint_count"].mean().reindex(range(7)).reset_index()
                dow["day"] = WEEKDAYS
                f = px.bar(
                    dow, x="day", y="complaint_count",
                    color=["Weekend" if i >= 5 else "Weekday" for i in range(7)],
                    color_discrete_map={"Weekday": DARK_BLUE, "Weekend": ORANGE},
                )
                f.update_traces(hovertemplate="%{x}: %{y:,.0f} requests per day<extra></extra>")
                f.update_layout(xaxis_title=None, yaxis_title="Average requests per day", legend_title=None)
                show(style_fig(f, 380))
            else:
                st.info("Choose a Month or All time scope to see weekday patterns (needs at least 7 days).")

        st.markdown("#### What does each borough mostly ask for?")
        pv = cur.pivot_table(index="service_area", columns="borough", values="complaint_count",
                             aggfunc="sum", fill_value=0)
        share = pv.div(pv.sum(axis=0).replace(0, np.nan), axis=1) * 100
        order = share.mean(axis=1).sort_values(ascending=False).index
        share = share.loc[order]
        f = go.Figure(go.Heatmap(
            z=share.values, x=share.columns.tolist(), y=share.index.tolist(),
            colorscale="Blues", text=np.round(share.values, 0), texttemplate="%{text:.0f}%",
            hovertemplate="%{x} | %{y}: %{z:.1f}% of that borough's requests<extra></extra>",
            colorbar=dict(title="% of borough"),
        ))
        f.update_yaxes(autorange="reversed")
        show(style_fig(f, 420))
        st.caption("Each column adds up to 100%: it shows the mix of services within a borough, so boroughs of different size can be compared.")

# =============================================================================
# 7. WEATHER & DEMAND
# =============================================================================
with tab_weather:
    if WEATHER_MIN is None:
        banner()
        st.info("No weather data is available in the Gold demand table.")
    else:
        banner(f"🌦️ Weather data exists for {fmt_date(WEATHER_MIN)} - {fmt_date(WEATHER_MAX)} only")
        w_start = max(scope.start, WEATHER_MIN)
        w_end = min(scope.end, WEATHER_MAX)
        if w_start > w_end:
            st.info(
                f"The selected scope has no weather data. Weather is only available for "
                f"{fmt_date(WEATHER_MIN)} to {fmt_date(WEATHER_MAX)}. "
                "Pick that period (for example the latest month) to see this page."
            )
        else:
            if (w_start, w_end) != (scope.start, scope.end):
                warning_banner(
                    f"Only the part of your scope that has weather data is used here: "
                    f"<b>{fmt_date(w_start)} to {fmt_date(w_end)}</b>."
                )
            areas = ["All service areas"] + sorted(demand["service_area"].unique().tolist())
            focus = st.selectbox(
                "Focus on a service area", areas,
                help="Applies to the first three charts. The last comparison always shows all areas.",
            )

            # Weather per (day, weather grid cell): one row each, taken from the mart
            wx = (
                demand[demand["total_precipitation_mm"].notna()]
                .groupby(["date", "grid_id"], as_index=False)
                .agg(
                    rain=("total_precipitation_mm", "first"),
                    wind=("max_wind_gust_ms", "first"),
                    temp=("avg_temperature_c", "first"),
                )
            )
            grids_in_scope = set(dem_b["grid_id"].unique())
            wx = wx[wx["grid_id"].isin(grids_in_scope)]
            wx = wx[(wx["date"] >= pd.Timestamp(w_start)) & (wx["date"] <= pd.Timestamp(w_end))]
            wx["condition"] = pd.cut(
                wx["rain"], [-np.inf, 1.0, STORM_MM, np.inf], right=False, labels=WEATHER_CONDITIONS
            )
            idx = pd.MultiIndex.from_frame(wx[["date", "grid_id"]])

            def calls_per_cell_day(area: Optional[str]) -> pd.Series:
                sub = window(dem_b, "date", w_start, w_end)
                if area:
                    sub = sub[sub["service_area"] == area]
                s = sub.groupby(["date", "grid_id"])["complaint_count"].sum()
                return s.reindex(idx, fill_value=0).to_numpy()

            wx["calls"] = calls_per_cell_day(None if focus == "All service areas" else focus)
            wx["day_type"] = np.where(wx["date"].dt.dayofweek >= 5, "Weekend", "Weekday")

            if len(wx) < 5:
                st.info("Not enough weather observations in this selection for a reliable comparison.")
            else:
                n_cells = wx["grid_id"].nunique()
                st.caption(
                    f"Unit of analysis: one **weather grid cell on one day** (about 10 km x 10 km). "
                    f"{len(wx):,} cell-days across {n_cells} cells and "
                    f"{wx['date'].nunique()} days. 'Rain' = total rainfall that day in that cell."
                )

                cond = wx.groupby("condition", observed=True).agg(
                    avg_calls=("calls", "mean"), cell_days=("calls", "size")
                ).reindex(WEATHER_CONDITIONS)
                dry = cond.loc[WEATHER_CONDITIONS[0], "avg_calls"]
                wet_mask = wx["condition"] != WEATHER_CONDITIONS[0]
                wet_avg = wx.loc[wet_mask, "calls"].mean() if wet_mask.any() else np.nan

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Rainiest day", f"{wx.groupby('date')['rain'].mean().max():.1f} mm",
                          help="Highest average rainfall across the weather grid cells on a single day.")
                m2.metric("Cell-days with rain (1 mm+)", f"{safe_div(wet_mask.sum(), len(wx), 100):.0f}%")
                m3.metric(
                    "Requests per cell-day, dry", f"{dry:.1f}" if pd.notna(dry) else "n/a",
                    help="Average requests per weather grid cell per day when it was dry (under 1 mm).",
                )
                m4.metric(
                    "Requests per cell-day, rainy",
                    f"{wet_avg:.1f}" if pd.notna(wet_avg) else "n/a",
                    f"{(wet_avg / dry - 1) * 100:+.0f}% vs dry" if pd.notna(wet_avg) and dry else None,
                    delta_color="off",
                    help="Same measure on days with 1 mm or more of rain.",
                )

                # ---- Daily timeline: rain vs requests
                st.markdown("#### Do request volumes move with the rain?")
                daily = wx.groupby("date").agg(rain=("rain", "mean"), calls=("calls", "sum")).reset_index()
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                fig.add_bar(
                    x=daily["date"], y=daily["rain"], name="Rain (avg across cells, mm)",
                    marker_color=BLUE, opacity=0.55,
                    hovertemplate="%{x|%a %d %b}<br>%{y:.1f} mm<extra></extra>",
                    secondary_y=True,
                )
                fig.add_scatter(
                    x=daily["date"], y=daily["calls"], name="Requests received",
                    line=dict(color=DARK_BLUE, width=3), mode="lines+markers",
                    hovertemplate="%{x|%a %d %b}<br>%{y:,} requests<extra></extra>",
                    secondary_y=False,
                )
                fig.update_yaxes(title_text="Requests per day", rangemode="tozero", secondary_y=False)
                fig.update_yaxes(title_text="Rainfall (mm)", rangemode="tozero", showgrid=False, secondary_y=True)
                show(style_fig(fig, 380))
                st.caption("Bars = rain, line = requests. Totals cover only grid cells with weather data.")

                # ---- Scatter + condition bars
                s1, s2 = st.columns(2)
                with s1:
                    st.markdown("#### More rain, more requests?")
                    f = px.scatter(
                        wx, x="rain", y="calls", color="condition", opacity=0.6,
                        category_orders={"condition": WEATHER_CONDITIONS},
                        color_discrete_map=dict(zip(WEATHER_CONDITIONS, [GREY, BLUE, RED])),
                        hover_data={"date": "|%d %b", "grid_id": True, "rain": ":.1f", "calls": True},
                        labels={"rain": "Rain that day (mm)", "calls": "Requests that day", "condition": ""},
                    )
                    if wx["rain"].nunique() > 2:
                        slope, intercept = np.polyfit(wx["rain"], wx["calls"], 1)
                        xs = np.array([wx["rain"].min(), wx["rain"].max()])
                        f.add_scatter(x=xs, y=slope * xs + intercept, mode="lines", name="Trend line",
                                      line=dict(color="black", dash="dot"))
                        r = np.corrcoef(wx["rain"], wx["calls"])[0, 1]
                        trend_text = (
                            f"Each extra 10 mm of rain goes with about <b>{slope * 10:+.1f}</b> requests per cell-day "
                            f"(correlation r = {r:.2f}). This is an association, not proof of cause."
                        )
                    else:
                        trend_text = ""
                    show(style_fig(f, 380))
                    if trend_text:
                        takeaway(trend_text)
                with s2:
                    st.markdown("#### Average requests by weather type")
                    cb = cond.reset_index()
                    cb["label"] = cb.apply(
                        lambda r: f"{r['avg_calls']:.1f}" if pd.notna(r["avg_calls"]) else "no data", axis=1
                    )
                    f = px.bar(
                        cb, x="condition", y="avg_calls", text="label", color="condition",
                        color_discrete_map=dict(zip(WEATHER_CONDITIONS, [GREY, BLUE, RED])),
                        labels={"condition": "", "avg_calls": "Requests per cell-day"},
                    )
                    f.update_traces(textposition="outside")
                    f.update_layout(showlegend=False)
                    show(style_fig(f, 380))
                    st.caption(
                        "Number of cell-days behind each bar: "
                        + " | ".join(f"{c.split(' (')[0]} {int(cond.loc[c, 'cell_days'])}" for c in WEATHER_CONDITIONS if pd.notna(cond.loc[c, "cell_days"]))
                    )

                # ---- Which services react to rain?
                st.markdown("#### Which services react most to rain?")
                sub = window(dem_b, "date", w_start, w_end)
                per_area = sub.groupby(["service_area", "date", "grid_id"])["complaint_count"].sum()
                rows = []
                is_dry = (wx["condition"] == WEATHER_CONDITIONS[0]).to_numpy()
                for area in sorted(sub["service_area"].unique()):
                    vals = per_area.loc[area].reindex(idx, fill_value=0).to_numpy()
                    d_avg = vals[is_dry].mean() if is_dry.any() else np.nan
                    w_avg = vals[~is_dry].mean() if (~is_dry).any() else np.nan
                    if pd.notna(d_avg) and d_avg >= 0.2 and (~is_dry).sum() >= 5:
                        rows.append({"service_area": area, "change": (w_avg / d_avg - 1) * 100,
                                     "dry": d_avg, "wet": w_avg})
                if rows:
                    sens = pd.DataFrame(rows).sort_values("change")
                    sens["direction"] = np.where(sens["change"] >= 0, "More requests when rainy", "Fewer requests when rainy")
                    f = px.bar(
                        sens, x="change", y="service_area", orientation="h", color="direction",
                        color_discrete_map={"More requests when rainy": ORANGE, "Fewer requests when rainy": GREY},
                        hover_data={"dry": ":.1f", "wet": ":.1f", "change": ":.0f", "direction": False},
                        labels={"change": "Change in requests per cell-day on rainy days (%)",
                                "service_area": "", "dry": "Dry avg", "wet": "Rainy avg"},
                    )
                    f.add_vline(x=0, line_color="black")
                    f.update_layout(legend_title=None)
                    show(style_fig(f, 400))
                    top_up = sens.iloc[-1]
                    takeaway(
                        f"<b>{top_up['service_area']}</b> reacts most to rain: "
                        f"{top_up['change']:+.0f}% requests per cell-day on rainy days vs dry days "
                        f"({top_up['dry']:.1f} to {top_up['wet']:.1f})."
                    )
                else:
                    st.info("Not enough rainy days in this scope to compare services.")

                # ---- Temperature
                st.markdown("#### Does temperature matter?")
                f = px.scatter(
                    wx, x="temp", y="calls", color="day_type", opacity=0.55,
                    color_discrete_map={"Weekday": DARK_BLUE, "Weekend": ORANGE},
                    labels={"temp": "Average temperature that day (°C)", "calls": "Requests that day", "day_type": ""},
                )
                show(style_fig(f, 340))
                st.caption("Weekend points are shown separately because weekends have a different baseline of requests.")

# =============================================================================
# 8. AGENCY PERFORMANCE
# =============================================================================
with tab_agency:
    banner("Tickets are counted on the day they were closed (closures) or opened (new tickets)")
    all_agencies = sorted(perf["agency"].unique().tolist())
    with st.expander("Filter agencies", expanded=False):
        sel_agencies = st.multiselect(
            "Agencies", all_agencies, default=all_agencies,
            format_func=lambda a: AGENCY_LABELS.get(a, a),
        ) or all_agencies

    pb = perf_b[perf_b["agency"].isin(sel_agencies)]
    pc = window(pb, "reporting_date", scope.start, scope.end)
    pp = window(pb, "reporting_date", scope.prev_start, scope.prev_end) if scope.prev_start else None

    def closure_stats(frame: pd.DataFrame) -> dict:
        closed = frame["tickets_closed_today"].sum()
        fv = frame["field_verified_closed_today"].sum()
        ph = frame["phantom_closed_today"].sum()
        return {
            "opened": frame["tickets_created_today"].sum(),
            "closed": closed,
            "fixed_pct": safe_div(fv, closed, 100),
            "nofix_pct": safe_div(ph, closed, 100),
            "hours": wmedian(frame["median_resolution_duration_hours"], frame["tickets_closed_today"]),
        }

    def backlog_snapshot(frame_all: pd.DataFrame, as_of: date) -> pd.DataFrame:
        """Latest known backlog per (borough, agency) on or before `as_of`."""
        sub = frame_all[frame_all["reporting_date"] <= pd.Timestamp(as_of)]
        return sub.sort_values("reporting_date").groupby(["borough", "agency"]).tail(1)

    cs = closure_stats(pc)
    ps_ = closure_stats(pp) if pp is not None else None
    snap = backlog_snapshot(pb, scope.end)
    open_now = snap["active_open_backlog_count"].sum()
    old_now = snap["long_open_backlog_count"].sum()
    snap_prev = backlog_snapshot(pb, scope.prev_end) if scope.prev_end else None

    if cs["closed"] == 0 and cs["opened"] == 0:
        st.info("No ticket activity for this selection. Try a wider time scope.")
    else:
        a1, a2, a3, a4, a5 = st.columns(5)
        a1.metric(
            "Tickets closed", fmt_int(cs["closed"]),
            delta_pct(cs["closed"], ps_["closed"] if ps_ else None, scope.prev_label), delta_color="off",
            help="Tickets that were closed during the period (whenever they were opened).",
        )
        a2.metric(
            "Typical time to close", fmt_hours(cs["hours"]),
            help="Middle value (median) of the time from opening to closing; the daily medians are "
                 "combined weighted by tickets closed. Medians are used because a few very old tickets "
                 "would distort an average.",
        )
        a3.metric(
            "Closed with a confirmed fix", f"{cs['fixed_pct']:.1f}%",
            delta_pts(cs["fixed_pct"], ps_["fixed_pct"] if ps_ else None, scope.prev_label), delta_color="normal",
            help="Closing note describes verified action on site (e.g. violation corrected, repaired).",
        )
        a4.metric(
            "Closed without a confirmed fix", f"{cs['nofix_pct']:.1f}%",
            delta_pts(cs["nofix_pct"], ps_["nofix_pct"] if ps_ else None, scope.prev_label), delta_color="inverse",
            help="Closed, but the note does not show a verified fix: e.g. no violation found, duplicate, "
                 "dismissed, or could not access. Not always a failure - but residents may not see a result.",
        )
        a5.metric(
            "Open tickets (end of period)", fmt_int(open_now),
            delta_pct(open_now, snap_prev["active_open_backlog_count"].sum() if snap_prev is not None else None, scope.prev_label),
            delta_color="inverse",
            help=f"Tickets still open at the end of the period. {safe_div(old_now, open_now, 100):.0f}% "
                 f"({fmt_int(old_now)}) have been open for more than {LONG_OPEN_DAYS} days.",
        )

        # ---- Opened vs closed trend
        t_start, t_end, shade, t_note = trend_window()
        flow = daily_totals(pb, "reporting_date", ["tickets_created_today", "tickets_closed_today"])
        flow["opened7"] = flow["tickets_created_today"].rolling(7, min_periods=1).mean()
        flow["closed7"] = flow["tickets_closed_today"].rolling(7, min_periods=1).mean()
        fv_ = flow.loc[pd.Timestamp(t_start):pd.Timestamp(t_end)].reset_index()
        st.markdown("#### Is the city keeping up? New tickets vs tickets closed")
        fig = go.Figure()
        fig.add_scatter(x=fv_["date"], y=fv_["opened7"], name="New tickets (7-day avg)",
                        line=dict(color=ORANGE, width=3),
                        hovertemplate="%{x|%a %d %b}<br>%{y:,.0f} opened/day<extra></extra>")
        fig.add_scatter(x=fv_["date"], y=fv_["closed7"], name="Tickets closed (7-day avg)",
                        line=dict(color=GREEN, width=3),
                        hovertemplate="%{x|%a %d %b}<br>%{y:,.0f} closed/day<extra></extra>")
        if shade:
            fig.add_vrect(x0=pd.Timestamp(shade) - pd.Timedelta(hours=12),
                          x1=pd.Timestamp(shade) + pd.Timedelta(hours=12),
                          fillcolor=ORANGE, opacity=0.2, line_width=0)
        fig.update_yaxes(title="Tickets per day", rangemode="tozero")
        show(style_fig(fig, 340))
        if t_note:
            st.caption(t_note)
        st.caption("When the green line sits below the orange line, the open backlog is growing.")

        # ---- Per-agency table
        by_ag = []
        for ag, g in pc.groupby("agency"):
            row = closure_stats(g)
            row["agency"] = ag
            row["n_fixed"] = g["field_verified_closed_today"].sum()
            row["n_nofix"] = g["phantom_closed_today"].sum()
            row["n_other"] = max(row["closed"] - row["n_fixed"] - row["n_nofix"], 0)
            by_ag.append(row)
        ag_df = pd.DataFrame(by_ag)
        bl = snap.groupby("agency")[["active_open_backlog_count", "long_open_backlog_count"]].sum()
        if not ag_df.empty:
            ag_df = ag_df.merge(bl, left_on="agency", right_index=True, how="left").fillna(
                {"active_open_backlog_count": 0, "long_open_backlog_count": 0}
            )
            ag_df["name"] = ag_df["agency"].map(lambda a: AGENCY_LABELS.get(a, a))

            min_closed = st.slider(
                "Hide agencies with fewer closed tickets than", 0, 500, 50, step=10,
                help="Small agencies produce noisy percentages.",
            )
            big = ag_df[ag_df["closed"] >= min_closed].sort_values("closed", ascending=False)

            g1, g2 = st.columns(2)
            with g1:
                st.markdown("#### What happens when tickets are closed?")
                if big.empty:
                    st.info("No agency meets the minimum closed-ticket filter.")
                else:
                    top = big.head(12).iloc[::-1]
                    f = go.Figure()
                    for col, name, color in (
                        ("n_fixed", "Confirmed fix", GREEN),
                        ("n_other", "Outcome unclear", GREY),
                        ("n_nofix", "No confirmed fix", RED),
                    ):
                        f.add_bar(
                            y=top["name"], x=top[col] / top["closed"].replace(0, np.nan) * 100,
                            name=name, orientation="h", marker_color=color,
                            customdata=top[col],
                            hovertemplate="%{y}<br>" + name + ": %{x:.0f}% (%{customdata:,} tickets)<extra></extra>",
                        )
                    f.update_layout(barmode="stack", xaxis_title="% of tickets closed", yaxis_title=None)
                    show(style_fig(f, 430))
                    st.caption("Top 12 agencies by tickets closed. 'Outcome unclear' = closing note too generic to classify.")
            with g2:
                st.markdown("#### Fast vs. effective: who is where?")
                sc = big.dropna(subset=["hours"])
                if sc.empty:
                    st.info("Not enough data for this chart.")
                else:
                    f = px.scatter(
                        sc, x="hours", y="nofix_pct", size="closed", color="name",
                        hover_name="name", size_max=48, log_x=True,
                        hover_data={"hours": ":.1f", "nofix_pct": ":.1f", "closed": ":,", "name": False},
                        labels={"hours": "Typical time to close (hours, log scale)",
                                "nofix_pct": "% closed without a confirmed fix", "closed": "Tickets closed"},
                    )
                    f.update_layout(showlegend=False)
                    f.add_hline(y=cs["nofix_pct"], line_dash="dot", line_color=GREY,
                                annotation_text="all agencies", annotation_position="top left")
                    show(style_fig(f, 430))
                    st.caption("Bubble size = tickets closed. Bottom-left = quick and with confirmed results.")

            h1, h2 = st.columns(2)
            with h1:
                st.markdown("#### Open tickets by agency (end of period)")
                bk = ag_df[ag_df["active_open_backlog_count"] > 0].copy()
                bk["recent"] = bk["active_open_backlog_count"] - bk["long_open_backlog_count"]
                bk = bk.nlargest(12, "active_open_backlog_count").iloc[::-1]
                if bk.empty:
                    st.info("No open tickets for this selection.")
                else:
                    f = go.Figure()
                    f.add_bar(y=bk["name"], x=bk["recent"], orientation="h", name=f"Open up to {LONG_OPEN_DAYS} days", marker_color=BLUE)
                    f.add_bar(y=bk["name"], x=bk["long_open_backlog_count"], orientation="h", name=f"Open over {LONG_OPEN_DAYS} days", marker_color=RED)
                    f.update_layout(barmode="stack", xaxis_title="Open tickets", yaxis_title=None)
                    show(style_fig(f, 400))
            with h2:
                st.markdown("#### Where are closures least often confirmed?")
                hm = pc.groupby(["agency", "borough"])[["tickets_closed_today", "phantom_closed_today"]].sum().reset_index()
                hm = hm[hm["tickets_closed_today"] >= 50]
                hm["rate"] = hm["phantom_closed_today"] / hm["tickets_closed_today"] * 100
                if hm.empty:
                    st.info("Not enough closures per agency and borough in this scope (needs 50+).")
                else:
                    pv = hm.pivot(index="agency", columns="borough", values="rate")
                    pv.index = [AGENCY_LABELS.get(a, a) for a in pv.index]
                    pv = pv.loc[pv.mean(axis=1).sort_values(ascending=False).index]
                    f = go.Figure(go.Heatmap(
                        z=pv.values, x=pv.columns.tolist(), y=pv.index.tolist(), colorscale="Reds",
                        text=np.round(pv.values, 0), texttemplate="%{text:.0f}%",
                        hovertemplate="%{y} in %{x}: %{z:.0f}% closed without confirmed fix<extra></extra>",
                        colorbar=dict(title="%"),
                    ))
                    f.update_yaxes(autorange="reversed")
                    show(style_fig(f, 400))
                    st.caption("Only agency/borough pairs with 50+ closures are shown.")

            st.markdown("#### Agency scorecard")
            table = pd.DataFrame({
                "Agency": ag_df["name"],
                "Tickets closed": ag_df["closed"].astype(int),
                "Typical time to close (hours)": ag_df["hours"].round(1),
                "Closed with confirmed fix (%)": ag_df["fixed_pct"].round(1),
                "Closed without confirmed fix (%)": ag_df["nofix_pct"].round(1),
                "Open tickets": ag_df["active_open_backlog_count"].astype(int),
                "Open over 7 days": ag_df["long_open_backlog_count"].astype(int),
            }).sort_values("Tickets closed", ascending=False)
            st.dataframe(
                table, hide_index=True, width="stretch",
                column_config={
                    "Closed with confirmed fix (%)": st.column_config.ProgressColumn(
                        format="%.1f%%", min_value=0, max_value=100),
                    "Closed without confirmed fix (%)": st.column_config.ProgressColumn(
                        format="%.1f%%", min_value=0, max_value=100),
                },
            )
            st.caption(
                "Open-ticket numbers use each borough and agency's latest known figure on or before the end "
                "of the period (the source only records backlog on days with activity)."
            )

# =============================================================================
# 9. NEIGHBORHOODS (ZIP) - whole-period mart, no date column
# =============================================================================
with tab_zip:
    banner()
    warning_banner(
        f"<b>This page always covers the whole data period ({fmt_date(DATA_MIN)} to {fmt_date(DATA_MAX)})</b> - "
        "the ZIP table is not split by date, so the time scope in the sidebar does not apply here. "
        "The borough selection does."
    )
    z_all = zips[zips["borough"].isin(selected_boroughs)]
    types = ["All complaint types"] + sorted(z_all["complaint_type"].unique().tolist())
    z_type = st.selectbox("Complaint type", types)
    zf = z_all if z_type == "All complaint types" else z_all[z_all["complaint_type"] == z_type]

    if zf.empty:
        st.info("No ZIP-level data for this selection.")
    else:
        rows = []
        for zc, g in zf.groupby("incident_zip"):
            total = g["total_complaints"].sum()
            rows.append({
                "zip": zc,
                "borough": g.groupby("borough")["total_complaints"].sum().idxmax(),
                "complaints": total,
                "closed": g["closed_complaints"].sum(),
                "hours": wmedian(g["median_resolution_hours"], g["closed_complaints"]),
                "exact_pct": safe_div((g["exact_gps_share_pct"] * g["total_complaints"]).sum(), total),
            })
        zdf = pd.DataFrame(rows)
        zdf["closed_pct"] = zdf["closed"] / zdf["complaints"] * 100
        if centroids is not None:
            zdf = zdf.merge(centroids[["zip5", "centroid_lat", "centroid_lon"]],
                            left_on="zip", right_on="zip5", how="left")

        top_zip = zdf.loc[zdf["complaints"].idxmax()]
        z1, z2, z3, z4 = st.columns(4)
        z1.metric("ZIP codes", fmt_int(len(zdf)))
        z2.metric("Complaints", fmt_int(zdf["complaints"].sum()))
        z3.metric("Highest-volume ZIP", f"{top_zip['zip']}", f"{fmt_int(top_zip['complaints'])} complaints", delta_color="off")
        z4.metric(
            "Located by exact address", f"{safe_div((zdf['exact_pct'] * zdf['complaints']).sum(), zdf['complaints'].sum()):.1f}%",
            help="Share of complaints with exact coordinates. The rest were placed using ZIP or borough centres.",
        )

        st.markdown("#### Map of complaint volume")
        color_by = st.radio(
            "Colour the dots by", ["Complaints", "Typical time to close", "% closed"], horizontal=True,
            help="Dot size is always the number of complaints.",
        )
        color_col, color_title = {
            "Complaints": ("complaints", "Complaints"),
            "Typical time to close": ("hours", "Hours to close (median)"),
            "% closed": ("closed_pct", "% closed"),
        }[color_by]
        mp = zdf.dropna(subset=["centroid_lat", "centroid_lon"]).copy()
        if mp.empty:
            st.info("ZIP centroid reference data is missing, so the map can't be drawn.")
        else:
            hi = mp[color_col].quantile(0.95) if mp[color_col].notna().any() else None
            f = px.scatter_map(
                mp, lat="centroid_lat", lon="centroid_lon", size="complaints", color=color_col,
                size_max=28, zoom=9.2, center=dict(lat=40.72, lon=-73.95), map_style="carto-positron",
                color_continuous_scale="YlOrRd", range_color=(0, hi) if hi else None,
                hover_name="zip",
                hover_data={"borough": True, "complaints": ":,", "hours": ":.1f", "closed_pct": ":.1f",
                            "centroid_lat": False, "centroid_lon": False},
                labels={color_col: color_title, "hours": "Median hours to close", "closed_pct": "% closed"},
            )
            show(style_fig(f, 520))
            st.caption("Colour scale is capped at the 95th percentile so a few extreme ZIPs don't wash out the rest.")

        b1, b2 = st.columns(2)
        with b1:
            st.markdown("#### 15 ZIP codes with the most complaints")
            t15 = zdf.nlargest(15, "complaints").iloc[::-1]
            f = px.bar(t15, x="complaints", y="zip", orientation="h", color="borough",
                       labels={"complaints": "Complaints", "zip": "ZIP", "borough": ""})
            f.update_yaxes(type="category")
            show(style_fig(f, 460))
        with b2:
            st.markdown("#### Slowest-to-resolve ZIP codes")
            min_n = st.slider("Only ZIPs with at least this many closed complaints", 0, 2000, 200, step=50)
            slow = zdf[(zdf["closed"] >= min_n) & zdf["hours"].notna()].nlargest(15, "hours").iloc[::-1]
            if slow.empty:
                st.info("No ZIP meets this minimum.")
            else:
                f = px.bar(slow, x="hours", y="zip", orientation="h", color="borough",
                           labels={"hours": "Typical time to close (hours)", "zip": "ZIP", "borough": ""})
                f.update_yaxes(type="category")
                show(style_fig(f, 460))

        st.markdown("#### Is volume related to speed?")
        sc = zdf.dropna(subset=["hours"])
        sc = sc[sc["closed"] >= 50]
        if not sc.empty:
            f = px.scatter(
                sc, x="complaints", y="hours", color="borough", log_x=True, log_y=True, opacity=0.7,
                hover_name="zip",
                labels={"complaints": "Complaints (log scale)", "hours": "Typical time to close, hours (log scale)", "borough": ""},
            )
            show(style_fig(f, 380))
            st.caption("Each dot is a ZIP code with 50+ closed complaints.")

        with st.expander("ZIP table"):
            tbl = zdf[["zip", "borough", "complaints", "closed_pct", "hours", "exact_pct"]].rename(columns={
                "zip": "ZIP", "borough": "Borough", "complaints": "Complaints", "closed_pct": "% closed",
                "hours": "Typical hours to close", "exact_pct": "% exact location",
            }).sort_values("Complaints", ascending=False)
            st.dataframe(tbl.round(1), hide_index=True, width="stretch")

# =============================================================================
# 10. DATA QUALITY
# =============================================================================
with tab_quality:
    report, taxonomy, resolutions = load_quality()
    banner()
    warning_banner(
        "<b>This page is about the data pipeline, not the time scope.</b> The top section describes the "
        "<b>latest pipeline run</b>; the reconciliation section covers <b>all data</b>."
    )

    st.markdown("#### Do the Gold tables agree with each other? (all data)")
    total_dem = int(demand["complaint_count"].sum())
    total_perf = int(perf["tickets_created_today"].sum())
    total_zip = int(zips["total_complaints"].sum())
    q1, q2, q3 = st.columns(3)
    q1.metric("Requests in demand table", fmt_int(total_dem))
    q2.metric("Tickets opened in agency table", fmt_int(total_perf),
              "matches" if total_perf == total_dem else f"{total_perf - total_dem:+,} difference",
              delta_color="off" if total_perf == total_dem else "inverse")
    q3.metric("Complaints in ZIP table", fmt_int(total_zip),
              "matches" if total_zip == total_dem else f"{total_zip - total_dem:+,} difference",
              delta_color="off" if total_zip == total_dem else "inverse",
              help="Small differences are expected: requests without a usable ZIP cannot appear in the ZIP table.")

    if report and "stages" in report:
        stg = report["stages"].get("311_cleaning", {})
        gen = report.get("generated_at_utc", "unknown")
        st.markdown(f"#### Latest pipeline run (report generated {gen})")
        raw_in = stg.get("input_rows", 0)
        accepted = stg.get("valid_rows_accepted", 0)
        rejected = stg.get("quarantined_rows", 0) + stg.get("duplicate_rows_removed", 0)
        r1, r2, r3 = st.columns(3)
        r1.metric("Records received", fmt_int(raw_in))
        r2.metric("Records accepted", fmt_int(accepted), f"{safe_div(accepted, raw_in, 100):.2f}% of received", delta_color="off")
        r3.metric("Records rejected", fmt_int(rejected),
                  help="Rejected = unusable timestamps/keys or exact duplicate rows.", delta_color="off")
        monthly = demand.groupby(demand["date"].dt.to_period("M"))["complaint_count"].sum()
        hit = monthly[monthly == accepted]
        if len(hit):
            st.success(f"✅ Accepted records ({fmt_int(accepted)}) match exactly the {hit.index[0].strftime('%B %Y')} requests in the demand table.")
        if stg.get("total_lakehouse_records"):
            ok = int(stg["total_lakehouse_records"]) == total_dem
            (st.success if ok else st.warning)(
                f"{'✅' if ok else '⚠️'} Pipeline reports {fmt_int(stg['total_lakehouse_records'])} records in the cleaned data overall; "
                f"the demand table holds {fmt_int(total_dem)}."
            )

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### How precisely were requests located?")
            spatial = stg.get("spatial_resolution_breakdown", {})
            if spatial:
                names = {"EXACT_COORDINATES": "Exact coordinates", "ZIP_APPROXIMATION": "Placed at ZIP centre",
                         "BOROUGH_APPROXIMATION": "Placed at borough centre", "UNLOCATED": "No location"}
                sp = pd.DataFrame({"type": [names.get(k, k) for k in spatial], "n": list(spatial.values())})
                f = px.pie(sp, names="type", values="n", hole=0.55, color_discrete_sequence=px.colors.qualitative.Safe)
                f.update_traces(textinfo="percent", hovertemplate="%{label}: %{value:,}<extra></extra>")
                show(style_fig(f, 340))
        with c2:
            st.markdown("#### How did we spot repeat reports?")
            routes = stg.get("incident_clustering", {}).get("route_breakdown", {})
            if routes:
                rt = pd.DataFrame({"how": [k.split(". ", 1)[-1] for k in routes], "n": list(routes.values())})
                rt = rt.sort_values("n")
                f = px.bar(rt, x="n", y="how", orientation="h", color_discrete_sequence=[DARK_BLUE],
                           labels={"n": "Requests", "how": ""})
                f.update_xaxes(type="log")
                show(style_fig(f, 340))
                st.caption("Requests are matched to a place by building, intersection, road segment, etc. (log scale).")

        c3, c4 = st.columns(2)
        with c3:
            st.markdown("#### How were tickets closed?")
            outcome = stg.get("resolution_text_mining", {}).get("outcome_breakdown", {})
            if outcome:
                names = {
                    "ACTION_TAKEN_REMEDIED": "Fixed on site", "OPEN_TICKET": "Still open",
                    "DUPLICATE_CONSOLIDATED": "Duplicate of another", "ACCESS_DENIED_UNABLE_TO_LOCATE": "Could not access / locate",
                    "ADMINISTRATIVE_DISMISSAL": "Dismissed administratively", "NO_VIOLATION_FOUND": "No violation found",
                    "UNCLASSIFIED_OR_GENERIC": "Note too generic to classify", "DEFERRED_PENDING_ACTION": "Deferred / pending action",
                    "UNSTATED_OR_EMPTY": "No closing note",
                }
                oc = pd.DataFrame({"outcome": [names.get(k, k) for k in outcome], "n": list(outcome.values())}).sort_values("n")
                f = px.bar(oc, x="n", y="outcome", orientation="h", color_discrete_sequence=[BLUE],
                           labels={"n": "Tickets", "outcome": ""})
                show(style_fig(f, 380))
        with c4:
            st.markdown("#### Were complaint types mapped to a service area?")
            sem = stg.get("semantic_conformance", {})
            if sem:
                st.metric("Mapped to a known service area", f"{sem.get('conformed_rate_pct', 0):.1f}%")
                st.metric("Fell into 'Other services'", fmt_int(sem.get("unmapped_records", 0)),
                          help="Complaint types we have not yet assigned to a specific service area.")
            wx_stage = report["stages"].get("weather_cleaning", {})
            join_stage = report["stages"].get("unified_join", {})
            if wx_stage or join_stage:
                st.markdown("**Weather feed**")
                st.write(
                    f"- {fmt_int(wx_stage.get('input_rows', 0))} hourly readings across "
                    f"{wx_stage.get('distinct_grid_cells', 0)} grid cells\n"
                    f"- {wx_stage.get('corrupted_fields_masked', 0)} implausible sensor values masked\n"
                    f"- {join_stage.get('weather_match_rate_pct', 0):.2f}% of requests matched to a weather reading"
                )
    else:
        st.info("No pipeline quality report found yet. Run the Silver pipeline to generate one.")

    st.markdown("#### Items waiting for human review")
    l1, l2 = st.columns(2)
    with l1:
        st.markdown("**Complaint types not yet mapped to a service area**")
        if taxonomy is not None and len(taxonomy):
            tx = taxonomy.nlargest(15, "occurrence_count")[["agency", "complaint_type", "occurrence_count", "review_status"]]
            st.dataframe(tx.rename(columns={"agency": "Agency", "complaint_type": "Complaint type",
                                            "occurrence_count": "Requests", "review_status": "Status"}),
                         hide_index=True, width="stretch")
            st.caption(f"{len(taxonomy)} unmapped types in total; top 15 by volume.")
        else:
            st.caption("Nothing pending.")
    with l2:
        st.markdown("**Closing notes too generic to classify**")
        if resolutions is not None and len(resolutions):
            rs = resolutions.nlargest(10, "occurrence_count")[["agency", "clean_resolution_text", "occurrence_count"]]
            rs["clean_resolution_text"] = rs["clean_resolution_text"].astype(str).str.slice(0, 110)
            st.dataframe(rs.rename(columns={"agency": "Agency", "clean_resolution_text": "Closing note (shortened)",
                                            "occurrence_count": "Tickets"}),
                         hide_index=True, width="stretch")
            st.caption(f"{len(resolutions)} distinct generic notes; top 10 by volume.")
        else:
            st.caption("Nothing pending.")
