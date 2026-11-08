from __future__ import annotations
import argparse, csv, json, time
from datetime import date, timedelta
from pathlib import Path
from common import *


def agg_exprs(delays, field='arrival_delay'):
    return ',\n'.join([
        f"count(*) FILTER (WHERE {field} <= {int(d)})::BIGINT AS o_{int(d)}"
        for d in delays
    ])


def read_split_segments(path: Path):
    """Return contiguous same-split, same-month date segments.

    The audit stage guarantees a chronological split over a contiguous selected
    run, but this routine remains defensive and starts a new segment on any gap,
    split change, year change, or month change.
    """
    rows=[]
    with Path(path).open('r', encoding='utf-8-sig', newline='') as f:
        for r in csv.DictReader(f):
            rows.append((date.fromisoformat(r['event_date']), r['split']))
    if not rows:
        raise RuntimeError('audit/split_days.csv is empty')
    rows.sort(key=lambda x:x[0])
    segs=[]
    start=prev=rows[0][0]; split=rows[0][1]
    for d,s in rows[1:]:
        contiguous=(d-prev).days==1
        same_bucket=(d.year==prev.year and d.month==prev.month)
        if s!=split or not contiguous or not same_bucket:
            segs.append((start,prev,split))
            start=d; split=s
        prev=d
    segs.append((start,prev,split))
    return segs


def eligible_cte(P, start: date, end: date, w: int, cell: float, speed_filter: str=''):
    # Explicit year/month predicates allow Hive partition pruning in core_parquet.
    if start.year!=end.year or start.month!=end.month:
        raise ValueError('snapshot chunk must not cross a calendar month')
    filt=(
        f"event_year={start.year} AND event_month={start.month} "
        f"AND event_date BETWEEN DATE {sql_str(start.isoformat())} AND DATE {sql_str(end.isoformat())} "
        f"AND valid_time AND valid_position AND valid_mmsi AND nonnegative_delay"
    )
    if speed_filter:
        filt += ' AND ' + speed_filter
    return f"""eligible AS (
      SELECT event_date,event_ts,processed_ts,mmsi,speed,valid_speed,
        CAST(floor(epoch(event_ts)/{w}) AS BIGINT) AS wid,
        CAST(floor(lat/{cell}) AS INTEGER) AS cy,
        CAST(floor(lon/{cell}) AS INTEGER) AS cx,
        CAST(extract(hour from event_ts) AS INTEGER) AS hour_of_day,
        greatest(0.0, epoch(processed_ts) - ((CAST(floor(epoch(event_ts)/{w}) AS BIGINT)+1)*{w})) AS arrival_delay
      FROM read_parquet({P}, hive_partitioning=true)
      WHERE {filt}
    )"""


def query_sql(kind, P, start, end, cfg):
    D=cfg['candidate_delays_seconds']; w=int(cfg['window_seconds']); cell=float(cfg['cell_degrees'])
    speed_thr=float(cfg['speed_threshold_knots']); max_speed=float(cfg['max_valid_speed_knots'])
    if kind=='message_count':
        cte=eligible_cte(P,start,end,w,cell)
        return f"""WITH {cte}
          SELECT event_date,wid,cx,cy,hour_of_day,
                 count(*)::BIGINT AS final_value,{agg_exprs(D)}
          FROM eligible
          GROUP BY event_date,wid,cx,cy,hour_of_day"""
    if kind=='distinct_vessels':
        cte=eligible_cte(P,start,end,w,cell)
        return f"""WITH {cte}, first_seen AS (
          SELECT event_date,wid,cx,cy,hour_of_day,mmsi,min(arrival_delay) AS arrival_delay
          FROM eligible
          GROUP BY event_date,wid,cx,cy,hour_of_day,mmsi
        )
        SELECT event_date,wid,cx,cy,hour_of_day,
               count(*)::BIGINT AS final_value,{agg_exprs(D)}
        FROM first_seen
        GROUP BY event_date,wid,cx,cy,hour_of_day"""
    if kind=='high_speed_vessels':
        sf=f"valid_speed AND speed >= {speed_thr} AND speed <= {max_speed}"
        cte=eligible_cte(P,start,end,w,cell,sf)
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
    s=str(exc).lower()
    return 'out of memory' in s or 'failed to allocate' in s


def done_marker(base: Path, kind: str, split: str, start: date, end: date):
    return base/'_chunks'/kind/split/f'{start:%Y%m%d}_{end:%Y%m%d}.done'


def copy_chunk(con, kind, P, base, split, start, end, cfg, requested_threads, depth=0):
    marker=done_marker(base,kind,split,start,end)
    if marker.exists():
        return 0
    target_dir=base/f'query={kind}'/f'split={split}'
    target_dir.mkdir(parents=True,exist_ok=True)
    target=target_dir/f'part_{start:%Y%m%d}_{end:%Y%m%d}.parquet'
    comp=str(cfg['parquet_compression']).upper(); rgs=int(cfg['row_group_size'])
    sql=query_sql(kind,P,start,end,cfg)
    copy=f"COPY ({sql}) TO {sql_str(str(target))} (FORMAT PARQUET, COMPRESSION {comp}, ROW_GROUP_SIZE {rgs})"
    indent='  '*depth
    print(f"{indent}{kind}: {split} {start}..{end}",flush=True)
    try:
        if target.exists(): target.unlink()
        con.execute(copy)
        marker.parent.mkdir(parents=True,exist_ok=True)
        marker.write_text('complete\n',encoding='ascii')
        return 1
    except Exception as e:
        if target.exists():
            try: target.unlink()
            except Exception: pass
        if not is_oom(e):
            raise
        if start < end:
            days=(end-start).days
            mid=start+timedelta(days=days//2)
            print(f"{indent}OOM -> splitting {start}..{end} into smaller date ranges",flush=True)
            n=copy_chunk(con,kind,P,base,split,start,mid,cfg,requested_threads,depth+1)
            n+=copy_chunk(con,kind,P,base,split,mid+timedelta(days=1),end,cfg,requested_threads,depth+1)
            marker.parent.mkdir(parents=True,exist_ok=True)
            marker.write_text('complete-via-children\n',encoding='ascii')
            return n
        # A single day is the irreducible unit. Retry it once with one thread.
        if requested_threads>1:
            print(f"{indent}OOM on one day -> retrying with threads=1",flush=True)
            con.execute("SET threads=1")
            try:
                if target.exists(): target.unlink()
                con.execute(copy)
                marker.parent.mkdir(parents=True,exist_ok=True)
                marker.write_text('complete-single-thread\n',encoding='ascii')
                return 1
            finally:
                con.execute("SET threads=?", [min(int(requested_threads),2)])
        raise


def run(out,cfg,memory_limit,threads):
    out=Path(out).resolve(); pq=out/'core_parquet'/'**/*.parquet'; snapdir=out/'snapshots'; data=snapdir/'data'
    data.mkdir(parents=True,exist_ok=True)
    requested_threads=int(threads)
    snapshot_threads=max(1,min(requested_threads,2))
    con=connect_duckdb(out,memory_limit,snapshot_threads)
    # ORDER is irrelevant for all snapshot aggregates. DuckDB recommends this
    # setting for large imports/exports and memory-constrained workloads.
    con.execute("SET preserve_insertion_order=false")
    started=time.perf_counter(); P=sql_str(str(pq))
    segments=read_split_segments(out/'audit'/'split_days.csv')
    kinds=['message_count','distinct_vessels','high_speed_vessels']
    physical_parts=0
    try:
        for kind in kinds:
            print(f'Building {kind} snapshots in bounded calendar chunks...',flush=True)
            for start,end,split in segments:
                physical_parts += copy_chunk(con,kind,P,data,split,start,end,cfg,snapshot_threads)
        G=sql_str(str(data/'**/*.parquet'))
        counts=con.execute(f"SELECT split,query,count(*) n FROM read_parquet({G}, hive_partitioning=true) GROUP BY split,query ORDER BY split,query").fetchall()
        man={
          "status":"complete",
          "elapsed_seconds":time.perf_counter()-started,
          "cell_windows_by_split_query":[{"split":a,"query":b,"cell_windows":int(c)} for a,b,c in counts],
          "candidate_delays_seconds":cfg['candidate_delays_seconds'],
          "window_seconds":int(cfg['window_seconds']),
          "cell_degrees":float(cfg['cell_degrees']),
          "queries":kinds,
          "storage":"wide: one row per nonempty cell-window-query; query and split are Hive path partitions",
          "execution":"calendar-month bounded chunks with automatic OOM bisection to smaller date ranges",
          "snapshot_threads":snapshot_threads,
          "preserve_insertion_order":False,
          "physical_parquet_parts_written_this_run":physical_parts,
          "resumable_chunk_markers":True
        }
        atomic_json(out/'manifests'/'snapshots.json',man)
        mark_success(out,'snapshots')
        print(json.dumps(man,indent=2),flush=True)
        return man
    finally:
        con.close()


def main():
    p=argparse.ArgumentParser();p.add_argument('output');p.add_argument('--config');p.add_argument('--memory-limit',default='8GB');p.add_argument('--threads',type=int,default=4);a=p.parse_args();run(a.output,merge_config(a.config),a.memory_limit,a.threads)
if __name__=='__main__':main()
