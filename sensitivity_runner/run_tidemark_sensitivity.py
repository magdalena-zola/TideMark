from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import sys
import time
import traceback
import zipfile
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# This script is meant to be placed next to common.py from TideMark v0.2.5.
from common import connect_duckdb, sql_str, atomic_json


VARIANTS = [
    {"name": "w15_o5",  "window_seconds": 900,  "offset_seconds": 300},
    {"name": "w15_o10", "window_seconds": 900,  "offset_seconds": 600},
    {"name": "w10_o0",  "window_seconds": 600,  "offset_seconds": 0},
    {"name": "w30_o0",  "window_seconds": 1800, "offset_seconds": 0},
    {"name": "w60_o0",  "window_seconds": 3600, "offset_seconds": 0},
]
BASELINE = {"name": "w15_o0", "window_seconds": 900, "offset_seconds": 0}
QUERIES = ["message_count", "distinct_vessels", "high_speed_vessels"]

LOG_PATH: Path | None = None


def log(msg=""):
    s = str(msg)
    print(s, flush=True)
    if LOG_PATH is not None:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(s + "\n")


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_json(path, obj)


def read_split_days(path: Path):
    rows = []
    split_map = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            d = date.fromisoformat(r["event_date"])
            s = r["split"]
            rows.append((d, s))
            split_map[d] = s
    if not rows:
        raise RuntimeError("audit/split_days.csv is empty")
    rows.sort(key=lambda x: x[0])
    return rows, split_map


def monthly_segments(split_rows):
    """Contiguous same-split chunks bounded by calendar month."""
    segs = []
    start = prev = split_rows[0][0]
    split = split_rows[0][1]
    for d, s in split_rows[1:]:
        contiguous = (d - prev).days == 1
        same_month = (d.year == prev.year and d.month == prev.month)
        if s != split or not contiguous or not same_month:
            segs.append((start, prev, split))
            start, split = d, s
        prev = d
    segs.append((start, prev, split))
    return segs


def month_keys(start: date, end: date):
    """Months touched by a source-date scan range."""
    keys = []
    cur = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    while cur <= last:
        keys.append((cur.year, cur.month))
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return keys


def agg_exprs(delays, field="arrival_delay"):
    return ",\n".join(
        f"count(*) FILTER (WHERE {field} <= {int(d)})::BIGINT AS o_{int(d)}"
        for d in delays
    )


def eligible_cte(parquet_glob_sql: str, start: date, end: date, split: str,
                 split_map: dict[date, str], window_seconds: int, offset_seconds: int,
                 cell_degrees: float, speed_filter: str = ""):
    """Build globally shifted windows while preserving chronological split integrity.

    A shifted window is assigned to the calendar date on which it starts.  For a
    non-zero offset, the last window of a day may cross midnight; we read the next
    source day so that window remains complete.  At train/calibration/test boundaries
    (and at the final corpus day), that cross-boundary window is excluded to prevent
    temporal leakage.
    """
    src_start = start
    src_end = end + timedelta(days=1) if offset_seconds > 0 else end
    month_pred = " OR ".join(
        f"(event_year={y} AND event_month={m})" for y, m in month_keys(src_start, src_end)
    )
    filt = (
        f"({month_pred}) AND event_date BETWEEN DATE {sql_str(src_start.isoformat())} "
        f"AND DATE {sql_str(src_end.isoformat())} "
        f"AND valid_time AND valid_position AND valid_mmsi AND nonnegative_delay"
    )
    if speed_filter:
        filt += " AND " + speed_filter

    next_day = end + timedelta(days=1)
    same_split_next = split_map.get(next_day) == split
    boundary_guard = ""
    if not same_split_next:
        # Exclude only a final cross-midnight shifted window at a split/corpus boundary.
        boundary_guard = (
            f"AND NOT (anchor_date = DATE {sql_str(end.isoformat())} "
            f"AND window_start_second + {window_seconds} > 86400)"
        )

    off = int(offset_seconds)
    w = int(window_seconds)
    cell = float(cell_degrees)
    return f"""shifted AS (
      SELECT event_date AS source_event_date,event_ts,processed_ts,mmsi,speed,valid_speed,
             lat,lon,
             CAST(event_ts - INTERVAL '{off} seconds' AS DATE) AS anchor_date
      FROM read_parquet({parquet_glob_sql}, hive_partitioning=true)
      WHERE {filt}
    ), indexed AS (
      SELECT *,
        CAST(floor((epoch(event_ts)-epoch(CAST(anchor_date AS TIMESTAMP))-{off})/{w}) AS BIGINT) AS wid_day
      FROM shifted
    ), windowed AS (
      SELECT *,
        ({off} + wid_day*{w})::BIGINT AS window_start_second
      FROM indexed
      WHERE anchor_date BETWEEN DATE {sql_str(start.isoformat())} AND DATE {sql_str(end.isoformat())}
    ), eligible AS (
      SELECT anchor_date AS event_date,event_ts,processed_ts,mmsi,speed,valid_speed,
        wid_day AS wid,
        CAST(floor(lat/{cell}) AS INTEGER) AS cy,
        CAST(floor(lon/{cell}) AS INTEGER) AS cx,
        CAST(floor(window_start_second/3600.0) AS INTEGER) AS hour_of_day,
        greatest(0.0,
          epoch(processed_ts) -
          (epoch(CAST(anchor_date AS TIMESTAMP)) + window_start_second + {w})
        ) AS arrival_delay
      FROM windowed
      WHERE true {boundary_guard}
    )"""


def query_sql(kind, parquet_glob_sql, start, end, split, split_map, cfg, variant):
    D = [int(x) for x in cfg["candidate_delays_seconds"]]
    w = int(variant["window_seconds"])
    off = int(variant["offset_seconds"])
    cell = float(cfg["cell_degrees"])
    speed_thr = float(cfg["speed_threshold_knots"])
    max_speed = float(cfg["max_valid_speed_knots"])

    if kind == "message_count":
        cte = eligible_cte(parquet_glob_sql, start, end, split, split_map, w, off, cell)
        return f"""WITH {cte}
          SELECT event_date,wid,cx,cy,hour_of_day,
                 count(*)::BIGINT AS final_value,{agg_exprs(D)}
          FROM eligible
          GROUP BY event_date,wid,cx,cy,hour_of_day"""

    if kind in ("distinct_vessels", "high_speed_vessels"):
        sf = ""
        if kind == "high_speed_vessels":
            sf = f"valid_speed AND speed >= {speed_thr} AND speed <= {max_speed}"
        cte = eligible_cte(parquet_glob_sql, start, end, split, split_map, w, off, cell, sf)
        return f"""WITH {cte}, first_seen AS (
          SELECT event_date,wid,cx,cy,hour_of_day,mmsi,min(arrival_delay) AS arrival_delay
          FROM eligible
          GROUP BY event_date,wid,cx,cy,hour_of_day,mmsi
        )
        SELECT event_date,wid,cx,cy,hour_of_day,
               count(*)::BIGINT AS final_value,{agg_exprs(D)}
        FROM first_seen
        GROUP BY event_date,wid,cx,cy,hour_of_day"""

    raise ValueError(kind)


def is_oom(exc: Exception):
    s = str(exc).lower()
    return "out of memory" in s or "failed to allocate" in s


def marker_path(work: Path, kind: str, split: str, start: date, end: date):
    return work / "_chunks" / kind / split / f"{start:%Y%m%d}_{end:%Y%m%d}.done"


def copy_chunk(con, kind, parquet_glob_sql, work, split, start, end, split_map,
               cfg, variant, requested_threads, depth=0):
    marker = marker_path(work, kind, split, start, end)
    if marker.exists():
        return 0

    target_dir = work / "data" / f"query={kind}" / f"split={split}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"part_{start:%Y%m%d}_{end:%Y%m%d}.parquet"
    comp = str(cfg["parquet_compression"]).upper()
    rgs = int(cfg["row_group_size"])
    sql = query_sql(kind, parquet_glob_sql, start, end, split, split_map, cfg, variant)
    copy = (
        f"COPY ({sql}) TO {sql_str(str(target))} "
        f"(FORMAT PARQUET, COMPRESSION {comp}, ROW_GROUP_SIZE {rgs})"
    )
    indent = "  " * depth
    log(f"{indent}{variant['name']} | {kind}: {split} {start}..{end}")
    try:
        if target.exists():
            target.unlink()
        con.execute(copy)
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("complete\n", encoding="ascii")
        return 1
    except Exception as e:
        if target.exists():
            try:
                target.unlink()
            except Exception:
                pass
        if not is_oom(e):
            raise
        if start < end:
            days = (end - start).days
            mid = start + timedelta(days=days // 2)
            log(f"{indent}OOM -> split {start}..{end}")
            n = copy_chunk(con, kind, parquet_glob_sql, work, split, start, mid,
                           split_map, cfg, variant, requested_threads, depth + 1)
            n += copy_chunk(con, kind, parquet_glob_sql, work, split,
                            mid + timedelta(days=1), end, split_map, cfg, variant,
                            requested_threads, depth + 1)
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text("complete-via-children\n", encoding="ascii")
            return n
        if requested_threads > 1:
            log(f"{indent}OOM one day -> retry threads=1")
            con.execute("SET threads=1")
            try:
                if target.exists():
                    target.unlink()
                con.execute(copy)
                marker.parent.mkdir(parents=True, exist_ok=True)
                marker.write_text("complete-single-thread\n", encoding="ascii")
                return 1
            finally:
                con.execute("SET threads=?", [min(int(requested_threads), 2)])
        raise


# ---- Evaluation logic: same policy definitions as TideMark v0.2.5 ----

def err_expr(d):
    d = int(d)
    return f"(final_value-o_{d})*1.0/final_value"


def selected_error_expr(delays, chosen="chosen_delay"):
    parts = " ".join(f"WHEN {int(d)} THEN {err_expr(d)}" for d in delays)
    return f"CASE {chosen} {parts} ELSE {err_expr(max(delays))} END"


def activity_case(edges):
    outer = []
    for q, qs in edges.items():
        cond = [f"WHEN o_0 <= {float(e)} THEN {i}" for i, e in enumerate(qs)]
        cond.append(f"ELSE {len(qs)}")
        outer.append(f"WHEN query={sql_str(q)} THEN CASE {' '.join(cond)} END")
    return "CASE " + " ".join(outer) + " ELSE 0 END"


def choose_from_summary(df, delays, alpha):
    good = df[df.risk <= alpha].sort_values("delay_seconds")
    return int(good.iloc[0].delay_seconds) if len(good) else int(max(delays))


def build_rules(con, delays, eps, alpha, min_context):
    specs = [
        (["query", "hour_of_day", "activity_bin"], "q_hour_activity"),
        (["query", "activity_bin"], "q_activity"),
        (["query"], "query"),
        ([], "global"),
    ]
    rules = []
    for keys, level in specs:
        selects = []
        for d in delays:
            ksel = (",".join(keys) + ",") if keys else ""
            kgroup = (" GROUP BY " + ",".join(keys)) if keys else ""
            selects.append(
                f"SELECT {ksel}{int(d)}::BIGINT delay_seconds,count(*)::BIGINT windows,"
                f"avg(CASE WHEN {err_expr(d)}>{float(eps)} THEN 1.0 ELSE 0.0 END) risk "
                f"FROM snap_enriched WHERE split='calibration'{kgroup}"
            )
        s = con.execute(" UNION ALL ".join(selects)).fetchdf()
        if keys:
            grouped = s.groupby(keys, dropna=False, sort=False)
            for key, g in grouped:
                if not isinstance(key, tuple):
                    key = (key,)
                n = int(g["windows"].max())
                if level not in ("query", "global") and n < int(min_context):
                    continue
                rec = {
                    "level": level,
                    "delay_seconds": choose_from_summary(g, delays, alpha),
                    "calibration_windows": n,
                }
                for k, v in zip(keys, key):
                    rec[k] = int(v) if k in ("hour_of_day", "activity_bin") else str(v)
                rules.append(rec)
        else:
            n = int(s["windows"].max())
            rules.append({
                "level": level,
                "delay_seconds": choose_from_summary(s, delays, alpha),
                "calibration_windows": n,
            })
    return pd.DataFrame(rules)


def make_rule_tables(con, rules):
    for level, keys in [
        ("q_hour_activity", ["query", "hour_of_day", "activity_bin"]),
        ("q_activity", ["query", "activity_bin"]),
        ("query", ["query"]),
        ("global", []),
    ]:
        x = rules[rules.level == level].copy()
        name = "r_" + level
        if x.empty:
            if keys:
                cols = ", ".join(
                    (["CAST(NULL AS VARCHAR) query"] if "query" in keys else [])
                    + (["CAST(NULL AS INTEGER) hour_of_day"] if "hour_of_day" in keys else [])
                    + (["CAST(NULL AS INTEGER) activity_bin"] if "activity_bin" in keys else [])
                    + ["CAST(NULL AS BIGINT) chosen_delay"]
                )
                con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT {cols} WHERE false")
            else:
                con.execute(f"CREATE OR REPLACE TEMP TABLE {name}(chosen_delay BIGINT)")
            continue
        keep = keys + ["delay_seconds"]
        x = x[keep].rename(columns={"delay_seconds": "chosen_delay"})
        con.register(name + "_df", x)
        con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM {name}_df")


def chosen_sql(delays, mode):
    maxd = int(max(delays))
    e = selected_error_expr(delays)
    if mode == "tidemark_context":
        choice = f"coalesce(r1.chosen_delay,r2.chosen_delay,r3.chosen_delay,r4.chosen_delay,{maxd})"
        joins = """LEFT JOIN r_q_hour_activity r1 ON b.query=r1.query AND b.hour_of_day=r1.hour_of_day AND b.activity_bin=r1.activity_bin
        LEFT JOIN r_q_activity r2 ON b.query=r2.query AND b.activity_bin=r2.activity_bin
        LEFT JOIN r_query r3 ON b.query=r3.query
        LEFT JOIN r_global r4 ON true"""
    elif mode == "query_static":
        choice = f"coalesce(r3.chosen_delay,{maxd})"
        joins = "LEFT JOIN r_query r3 ON b.query=r3.query"
    elif mode == "fixed_global":
        choice = f"coalesce(r4.chosen_delay,{maxd})"
        joins = "LEFT JOIN r_global r4 ON true"
    else:
        raise ValueError(mode)
    return f"""WITH b AS (SELECT * FROM snap_enriched WHERE split='test'), c AS (
      SELECT b.*,{choice}::BIGINT AS chosen_delay FROM b {joins})
      SELECT *,{e} AS selected_error FROM c"""


def aggregate_policy(con, base_sql, eps, policy, alpha):
    q = f"""SELECT {sql_str(policy)} AS policy_name,{float(eps)} AS epsilon,{float(alpha)} AS alpha,count(*)::BIGINT AS windows,
      avg(chosen_delay) AS mean_delay_seconds,quantile_cont(chosen_delay,0.5) AS median_delay_seconds,quantile_cont(chosen_delay,0.95) AS p95_delay_seconds,
      avg(CASE WHEN selected_error>{float(eps)} THEN 1.0 ELSE 0.0 END) AS violation_fraction,
      avg(CASE WHEN selected_error>0 THEN 1.0 ELSE 0.0 END) AS any_change_fraction,
      avg(selected_error) AS macro_mean_relative_error,quantile_cont(selected_error,0.95) AS p95_relative_error FROM ({base_sql}) AS selected_rows"""
    row = con.execute(q).fetchdf().rename(columns={"policy_name": "policy"})
    byq = con.execute(f"""SELECT {sql_str(policy)} AS policy_name,{float(eps)} AS epsilon,{float(alpha)} AS alpha,query,count(*)::BIGINT AS windows,avg(chosen_delay) AS mean_delay_seconds,
      avg(CASE WHEN selected_error>{float(eps)} THEN 1.0 ELSE 0.0 END) AS violation_fraction,avg(selected_error) AS macro_mean_relative_error,
      quantile_cont(selected_error,0.95) AS p95_relative_error FROM ({base_sql}) AS selected_rows GROUP BY query ORDER BY query""").fetchdf().rename(columns={"policy_name": "policy"})
    daily = con.execute(f"""SELECT event_date,{sql_str(policy)} AS policy_name,{float(eps)} AS epsilon,{float(alpha)} AS alpha,
      avg(chosen_delay) AS mean_delay_seconds,avg(CASE WHEN selected_error>{float(eps)} THEN 1.0 ELSE 0.0 END) AS violation_fraction,avg(selected_error) AS macro_mean_relative_error
      FROM ({base_sql}) AS selected_rows GROUP BY event_date ORDER BY event_date""").fetchdf().rename(columns={"policy_name": "policy"})
    return row, byq, daily


def oracle_sql(delays, eps):
    maxd = int(max(delays))
    terms = [f"WHEN {err_expr(d)} <= {float(eps)} THEN {int(d)}" for d in delays]
    choice = "CASE " + " ".join(terms) + f" ELSE {maxd} END"
    e = selected_error_expr(delays)
    return f"""WITH c AS (SELECT *,({choice})::BIGINT chosen_delay FROM snap_enriched WHERE split='test') SELECT *,{e} AS selected_error FROM c"""


def bootstrap_daily(daily, reps, seed):
    rows = []
    rng = np.random.default_rng(seed)
    for (policy, eps, alpha), g in daily.groupby(["policy", "epsilon", "alpha"], dropna=False):
        for metric in ["mean_delay_seconds", "violation_fraction", "macro_mean_relative_error"]:
            x = g[metric].to_numpy(float)
            if len(x) < 2:
                continue
            vals = np.array([rng.choice(x, size=len(x), replace=True).mean() for _ in range(reps)])
            rows.append({
                "policy": policy,
                "epsilon": eps,
                "alpha": alpha,
                "metric": metric,
                "estimate": float(x.mean()),
                "ci025": float(np.quantile(vals, .025)),
                "ci975": float(np.quantile(vals, .975)),
                "days": len(x),
            })
    return pd.DataFrame(rows)


def add_variant_cols(df: pd.DataFrame, variant):
    x = df.copy()
    x.insert(0, "offset_minutes", float(variant["offset_seconds"]) / 60.0)
    x.insert(0, "window_minutes", float(variant["window_seconds"]) / 60.0)
    x.insert(0, "variant", variant["name"])
    return x


def evaluate_variant(root: Path, work: Path, result_dir: Path, cfg, variant,
                     memory_limit: str, threads: int):
    started = time.perf_counter()
    G = sql_str(str(work / "data" / "**/*.parquet"))
    D = [int(x) for x in cfg["candidate_delays_seconds"]]
    con = connect_duckdb(work, memory_limit, max(1, min(int(threads), 2)))
    con.execute("SET preserve_insertion_order=false")
    try:
        edges = {}
        for q in QUERIES:
            vals = con.execute(
                f"SELECT quantile_cont(o_0,[0.25,0.5,0.75]) FROM read_parquet({G},hive_partitioning=true) "
                f"WHERE split='train' AND query={sql_str(q)}"
            ).fetchone()[0]
            edges[q] = sorted(set(float(x) for x in (vals or [])))
        write_json(result_dir / "activity_bin_edges.json", edges)
        acase = activity_case(edges)
        con.execute(
            f"CREATE TEMP VIEW snap_enriched AS SELECT *,({acase})::INTEGER activity_bin "
            f"FROM read_parquet({G},hive_partitioning=true)"
        )

        curve_parts = []
        for d in D:
            cols = [
                f"query,{int(d)}::BIGINT delay_seconds,count(*)::BIGINT cell_windows,"
                f"avg({err_expr(d)}) macro_mean_relative_error,"
                f"quantile_cont({err_expr(d)},0.95) p95_relative_error,"
                f"avg(CASE WHEN {err_expr(d)}=0 THEN 1.0 ELSE 0.0 END) exact_fraction"
            ]
            for eps in cfg["epsilons"]:
                cols.append(
                    f"avg(CASE WHEN {err_expr(d)}>{float(eps)} THEN 1.0 ELSE 0.0 END) "
                    f"AS violation_eps_{str(float(eps)).replace('.', '_')}"
                )
            curve_parts.append(
                f"SELECT {','.join(cols)} FROM snap_enriched WHERE split='test' GROUP BY query"
            )
        curve = con.execute(" UNION ALL ".join(curve_parts) + " ORDER BY query,delay_seconds").fetchdf()
        add_variant_cols(curve, variant).to_csv(result_dir / "test_delay_curves.csv", index=False)

        metrics, byq, daily_all, rules_all = [], [], [], []
        for eps in cfg["epsilons"]:
            for alpha in cfg["alphas"]:
                rules = build_rules(con, D, float(eps), float(alpha), int(cfg["min_context_windows"]))
                rules["epsilon"] = float(eps)
                rules["alpha"] = float(alpha)
                rules_all.append(rules)
                make_rule_tables(con, rules)
                for mode in ["fixed_global", "query_static", "tidemark_context"]:
                    bsql = chosen_sql(D, mode)
                    a, b, d = aggregate_policy(con, bsql, float(eps), mode, float(alpha))
                    metrics.append(a); byq.append(b); daily_all.append(d)
                osql = oracle_sql(D, float(eps))
                a, b, d = aggregate_policy(con, osql, float(eps), "oracle_offline", float(alpha))
                metrics.append(a); byq.append(b); daily_all.append(d)

        metrics_df = add_variant_cols(pd.concat(metrics, ignore_index=True), variant)
        byq_df = add_variant_cols(pd.concat(byq, ignore_index=True), variant)
        daily = pd.concat(daily_all, ignore_index=True)
        daily_v = add_variant_cols(daily, variant)
        metrics_df.to_csv(result_dir / "policy_metrics.csv", index=False)
        byq_df.to_csv(result_dir / "policy_metrics_by_query.csv", index=False)
        daily_v.to_csv(result_dir / "daily_policy_metrics.csv", index=False)
        if rules_all:
            add_variant_cols(pd.concat(rules_all, ignore_index=True), variant).to_csv(
                result_dir / "tidemark_rules.csv", index=False
            )
        boot = bootstrap_daily(daily, int(cfg["bootstrap_replicates"]), int(cfg["bootstrap_seed"]))
        add_variant_cols(boot, variant).to_csv(result_dir / "bootstrap_day_ci.csv", index=False)

        counts = con.execute(
            "SELECT split,query,count(*) n FROM snap_enriched GROUP BY split,query ORDER BY split,query"
        ).fetchdf()
        manifest = {
            "status": "complete",
            "variant": variant,
            "elapsed_seconds": time.perf_counter() - started,
            "cell_windows_by_split_query": [
                {"split": str(r.split), "query": str(r.query), "n": int(r.n)}
                for r in counts.itertuples(index=False)
            ],
            "split_integrity": "windows assigned by window-start date; cross-midnight window excluded only at train/calibration/test or corpus boundary",
            "evaluation": "same TideMark v0.2.5 policy definitions and chronological train/calibration/test protocol",
        }
        write_json(result_dir / "manifest.json", manifest)
        return manifest
    finally:
        con.close()


def build_variant(root: Path, cfg, variant, split_rows, split_map,
                  memory_limit: str, threads: int, keep_work: bool):
    sens = root / "sensitivity"
    result_dir = sens / "results" / variant["name"]
    done = result_dir / "manifest.json"
    if done.exists():
        try:
            if load_json(done).get("status") == "complete":
                log(f"[{variant['name']}] already complete; skipping")
                return
        except Exception:
            pass

    work = sens / "_work" / variant["name"] / "snapshots"
    work.mkdir(parents=True, exist_ok=True)
    segments = monthly_segments(split_rows)
    parquet_glob_sql = sql_str(str(root / "core_parquet" / "**/*.parquet"))
    snap_threads = max(1, min(int(threads), 2))
    con = connect_duckdb(work, memory_limit, snap_threads)
    con.execute("SET preserve_insertion_order=false")
    parts = 0
    started = time.perf_counter()
    try:
        for kind in QUERIES:
            log(f"[{variant['name']}] building {kind}")
            for start, end, split in segments:
                parts += copy_chunk(
                    con, kind, parquet_glob_sql, work, split, start, end,
                    split_map, cfg, variant, snap_threads
                )
    finally:
        con.close()

    log(f"[{variant['name']}] snapshots complete; evaluating")
    result_dir.mkdir(parents=True, exist_ok=True)
    m = evaluate_variant(root, work, result_dir, cfg, variant, memory_limit, threads)
    m["snapshot_build_elapsed_seconds"] = time.perf_counter() - started
    m["physical_parts_written_this_run"] = int(parts)
    write_json(result_dir / "manifest.json", m)

    if not keep_work:
        shutil.rmtree(sens / "_work" / variant["name"], ignore_errors=True)
        log(f"[{variant['name']}] heavy work snapshots removed after successful evaluation")


def copy_baseline(root: Path):
    src = root / "evaluation"
    if not (src / "policy_metrics.csv").exists():
        raise FileNotFoundError("Existing TideMark evaluation/policy_metrics.csv not found")
    dst = root / "sensitivity" / "results" / BASELINE["name"]
    dst.mkdir(parents=True, exist_ok=True)
    for name in [
        "policy_metrics.csv", "policy_metrics_by_query.csv", "daily_policy_metrics.csv",
        "tidemark_rules.csv", "bootstrap_day_ci.csv", "test_delay_curves.csv"
    ]:
        p = src / name
        if p.exists():
            df = pd.read_csv(p)
            add_variant_cols(df, BASELINE).to_csv(dst / name, index=False)
    edges = src / "activity_bin_edges.json"
    if edges.exists():
        shutil.copy2(edges, dst / "activity_bin_edges.json")
    write_json(dst / "manifest.json", {
        "status": "complete",
        "variant": BASELINE,
        "source": "existing TideMark_full/evaluation; not recomputed",
    })


def combine_results(root: Path, cfg):
    result_root = root / "sensitivity" / "results"
    variants = [BASELINE] + VARIANTS
    file_names = [
        "policy_metrics.csv", "policy_metrics_by_query.csv", "daily_policy_metrics.csv",
        "bootstrap_day_ci.csv", "test_delay_curves.csv", "tidemark_rules.csv"
    ]
    combined = {}
    for fn in file_names:
        frames = []
        for v in variants:
            p = result_root / v["name"] / fn
            if p.exists():
                frames.append(pd.read_csv(p))
        if frames:
            x = pd.concat(frames, ignore_index=True)
            out = root / "sensitivity" / ("all_" + fn)
            x.to_csv(out, index=False)
            combined[fn] = x

    pm = combined.get("policy_metrics.csv")
    if pm is not None and not pm.empty:
        rows = []
        keys = ["variant", "window_minutes", "offset_minutes", "epsilon", "alpha"]
        for key, g in pm.groupby(keys, dropna=False):
            rec = dict(zip(keys, key if isinstance(key, tuple) else (key,)))
            gg = g.set_index("policy")
            if "tidemark_context" not in gg.index:
                continue
            t = gg.loc["tidemark_context"]
            rec.update({
                "tidemark_mean_delay_seconds": float(t["mean_delay_seconds"]),
                "tidemark_violation_fraction": float(t["violation_fraction"]),
                "tidemark_within_alpha": bool(float(t["violation_fraction"]) <= float(rec["alpha"])),
            })
            for b in ["fixed_global", "query_static"]:
                if b in gg.index:
                    bb = gg.loc[b]
                    rec[f"{b}_mean_delay_seconds"] = float(bb["mean_delay_seconds"])
                    rec[f"{b}_violation_fraction"] = float(bb["violation_fraction"])
                    denom = float(bb["mean_delay_seconds"])
                    rec[f"delay_reduction_vs_{b}_pct"] = (
                        100.0 * (denom - float(t["mean_delay_seconds"])) / denom if denom > 0 else math.nan
                    )
            rows.append(rec)
        comp = pd.DataFrame(rows)
        comp.to_csv(root / "sensitivity" / "policy_comparison.csv", index=False)

        phase = comp[(comp["window_minutes"] == 15.0) & (comp["offset_minutes"].isin([0.0, 5.0, 10.0]))]
        phase.to_csv(root / "sensitivity" / "phase_robustness.csv", index=False)

        duration = comp[(comp["offset_minutes"] == 0.0) & (comp["window_minutes"].isin([10.0, 15.0, 30.0, 60.0]))]
        duration.to_csv(root / "sensitivity" / "window_size_robustness.csv", index=False)

    manifest = {
        "status": "complete",
        "variants": [BASELINE] + VARIANTS,
        "candidate_delays_seconds": cfg["candidate_delays_seconds"],
        "epsilons": cfg["epsilons"],
        "alphas": cfg["alphas"],
        "note": "15-min offset variants use globally shifted windows. Windows crossing a train/calibration/test boundary are excluded to prevent temporal leakage.",
    }
    write_json(root / "sensitivity" / "manifest.json", manifest)


def make_return_zip(root: Path):
    sens = root / "sensitivity"
    zpath = root / "TideMark_SENSITIVITY_RETURN.zip"
    if zpath.exists():
        zpath.unlink()
    include_roots = [sens / "results"]
    include_files = [
        sens / "manifest.json", sens / "sensitivity.log",
        sens / "all_policy_metrics.csv", sens / "all_policy_metrics_by_query.csv",
        sens / "all_bootstrap_day_ci.csv", sens / "all_test_delay_curves.csv",
        sens / "all_tidemark_rules.csv", sens / "policy_comparison.csv",
        sens / "phase_robustness.csv", sens / "window_size_robustness.csv",
    ]
    with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for base in include_roots:
            if base.exists():
                for p in base.rglob("*"):
                    if p.is_file():
                        z.write(p, p.relative_to(root))
        for p in include_files:
            if p.exists():
                z.write(p, p.relative_to(root))
    return zpath


def main():
    global LOG_PATH
    p = argparse.ArgumentParser(description="TideMark ICDE sensitivity/ablation study")
    p.add_argument("output", help="Existing TideMark_full directory")
    p.add_argument("--memory-limit", default="8GB")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--keep-work", action="store_true", help="Keep heavy variant snapshots after successful evaluation")
    p.add_argument("--only", choices=[v["name"] for v in VARIANTS], help="Run only one sensitivity variant")
    a = p.parse_args()

    root = Path(a.output).resolve()
    if not (root / "core_parquet").exists():
        raise FileNotFoundError(f"core_parquet not found under {root}")
    if not (root / "audit" / "split_days.csv").exists():
        raise FileNotFoundError("audit/split_days.csv not found")
    if not (root / "config_used.json").exists():
        raise FileNotFoundError("config_used.json not found")

    sens = root / "sensitivity"
    sens.mkdir(parents=True, exist_ok=True)
    LOG_PATH = sens / "sensitivity.log"
    log("=" * 78)
    log("TideMark sensitivity study")
    log(f"Root: {root}")
    log(f"Free disk: {shutil.disk_usage(root).free / (1024**3):.2f} GiB")
    log(f"DuckDB memory limit: {a.memory_limit}; requested threads: {a.threads}; snapshot threads <= 2")

    cfg = load_json(root / "config_used.json")
    split_rows, split_map = read_split_days(root / "audit" / "split_days.csv")
    copy_baseline(root)

    selected = [v for v in VARIANTS if a.only is None or v["name"] == a.only]
    try:
        for v in selected:
            free_gib = shutil.disk_usage(root).free / (1024**3)
            log(f"[{v['name']}] free disk before variant: {free_gib:.2f} GiB")
            if free_gib < 20:
                raise RuntimeError("Less than 20 GiB free disk; refusing to start another heavy sensitivity variant")
            build_variant(root, cfg, v, split_rows, split_map, a.memory_limit, a.threads, a.keep_work)
        combine_results(root, cfg)
        z = make_return_zip(root)
        log(f"COMPLETE: {z}")
        print(f"\nRETURN ZIP: {z}", flush=True)
    except Exception:
        tb = traceback.format_exc()
        log(tb)
        raise


if __name__ == "__main__":
    main()
