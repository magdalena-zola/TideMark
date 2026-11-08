from __future__ import annotations
import argparse, json, time
from pathlib import Path
from common import *

def run(out,cfg,memory_limit,threads):
    out=Path(out).resolve();pq=out/'core_parquet'/'**/*.parquet';audit=out/'audit';audit.mkdir(parents=True,exist_ok=True)
    con=connect_duckdb(out,memory_limit,threads);started=time.perf_counter()
    P=sql_str(str(pq))
    try:
        profile_sql=f"""SELECT
          count(*) AS row_count,
          min(event_ts) AS event_min, max(event_ts) AS event_max,
          min(processed_ts) AS processed_min, max(processed_ts) AS processed_max,
          sum(CASE WHEN NOT valid_time THEN 1 ELSE 0 END) AS invalid_time,
          sum(CASE WHEN NOT valid_position THEN 1 ELSE 0 END) AS invalid_position,
          sum(CASE WHEN NOT valid_mmsi THEN 1 ELSE 0 END) AS invalid_mmsi,
          sum(CASE WHEN valid_time AND NOT nonnegative_delay THEN 1 ELSE 0 END) AS negative_delay,
          sum(CASE WHEN delay_seconds > 3600 THEN 1 ELSE 0 END) AS delay_over_hour,
          sum(CASE WHEN delay_seconds > 86400 THEN 1 ELSE 0 END) AS delay_over_day,
          min(delay_seconds) FILTER (WHERE valid_time) AS delay_min_seconds,
          max(delay_seconds) FILTER (WHERE valid_time) AS delay_max_seconds,
          approx_quantile(delay_seconds,0.5) FILTER (WHERE valid_time AND nonnegative_delay) AS delay_p50_seconds,
          approx_quantile(delay_seconds,0.9) FILTER (WHERE valid_time AND nonnegative_delay) AS delay_p90_seconds,
          approx_quantile(delay_seconds,0.95) FILTER (WHERE valid_time AND nonnegative_delay) AS delay_p95_seconds,
          approx_quantile(delay_seconds,0.99) FILTER (WHERE valid_time AND nonnegative_delay) AS delay_p99_seconds,
          count(*) FILTER (WHERE movement_id IS NULL OR movement_id='') AS movement_id_missing,
          approx_count_distinct(movement_id) FILTER (WHERE movement_id IS NOT NULL AND movement_id<>'') AS movement_id_approx_distinct,
          approx_count_distinct(mmsi) FILTER (WHERE valid_mmsi) AS mmsi_approx_distinct
        FROM read_parquet({P}, hive_partitioning=true)"""
        cur=con.execute(profile_sql);row=cur.fetchone();cols=[d[0] for d in cur.description]
        prof=dict(zip(cols,row)); prof['rows']=int(prof.pop('row_count')); prof['status']='complete';prof['elapsed_seconds']=time.perf_counter()-started
        for k,v in list(prof.items()):
            if hasattr(v,'isoformat'):prof[k]=v.isoformat()
            elif isinstance(v,(int,float,str,bool)) or v is None: pass
            else:
                try: prof[k]=float(v)
                except Exception: prof[k]=str(v)
        atomic_json(audit/'global_profile.json',prof)
        daily_sql=f"""COPY (SELECT event_date, count(*) AS n_rows,
           count(*) FILTER (WHERE valid_time AND valid_position AND valid_mmsi AND nonnegative_delay) eligible_rows,
           approx_count_distinct(mmsi) FILTER (WHERE valid_mmsi) approx_vessels,
           avg(delay_seconds) FILTER (WHERE valid_time AND nonnegative_delay) mean_delay_seconds,
           approx_quantile(delay_seconds,0.50) FILTER (WHERE valid_time AND nonnegative_delay) p50_delay_seconds,
           approx_quantile(delay_seconds,0.95) FILTER (WHERE valid_time AND nonnegative_delay) p95_delay_seconds,
           approx_quantile(delay_seconds,0.99) FILTER (WHERE valid_time AND nonnegative_delay) p99_delay_seconds,
           count(*) FILTER (WHERE valid_time AND NOT nonnegative_delay) negative_delays
           FROM read_parquet({P}, hive_partitioning=true)
           WHERE event_date IS NOT NULL GROUP BY event_date ORDER BY event_date)
           TO {sql_str(str(audit/'daily_profile.csv'))} (HEADER, DELIMITER ',')"""
        con.execute(daily_sql)
        block_sql=f"""COPY (SELECT export_block_id,min(export_row_id) first_row,max(export_row_id) last_row,
           min(event_ts) event_min,max(event_ts) event_max,min(processed_ts) processed_min,max(processed_ts) processed_max,
           count(*) AS n_rows FROM read_parquet({P}, hive_partitioning=true)
           GROUP BY export_block_id ORDER BY export_block_id)
           TO {sql_str(str(audit/'export_blocks.csv'))} (HEADER, DELIMITER ',')"""
        con.execute(block_sql)
        choose_period(con,P,audit,cfg)
        if cfg.get('exact_movement_id_repetition',False):
            mid_sql=f"""COPY (SELECT count(*) AS movement_id_values_repeated,
              coalesce(sum(n-1),0) AS excess_rows_among_repeated_ids,max(n) AS max_rows_per_movement_id FROM (
                SELECT movement_id,count(*) n FROM read_parquet({P}, hive_partitioning=true)
                WHERE movement_id IS NOT NULL AND movement_id<>'' GROUP BY movement_id HAVING count(*)>1))
              TO {sql_str(str(audit/'movement_id_repetition.csv'))} (HEADER, DELIMITER ',')"""
            con.execute(mid_sql)
        else:
            write_csv_rows(audit/'movement_id_repetition.csv',['status','reason'],[{'status':'not_run','reason':'Exact full-corpus grouping disabled by config; use approximate distinct count in global_profile or enable exact_movement_id_repetition.'}])
        mark_success(out,'audit',{"rows":prof.get('rows')})
        print(json.dumps(prof,indent=2),flush=True)
        return prof
    finally:con.close()

def choose_period(con,P,audit,cfg):
    rows=con.execute(f"SELECT event_date, count(*) n FROM read_parquet({P}, hive_partitioning=true) WHERE event_date IS NOT NULL GROUP BY event_date ORDER BY event_date").fetchall()
    minrows=int(cfg['min_daily_rows']);good=[(d,int(n)) for d,n in rows if int(n)>=minrows]
    if not good: raise RuntimeError('No event days pass min_daily_rows; inspect audit/daily_profile.csv and lower config threshold if justified.')
    runs=[];cur=[];prev=None
    for d,n in good:
        if prev is None or (d-prev).days==1:cur.append((d,n))
        else:
            if cur:runs.append(cur)
            cur=[(d,n)]
        prev=d
    if cur:runs.append(cur)
    run=max(runs,key=lambda x:(len(x),sum(n for _,n in x)))
    target_days=int(cfg['min_experiment_days'])
    below_target=len(run)<target_days
    # Never merge disconnected calendar periods into a train/calibration/test split.
    # For smoke/prefix runs a short contiguous run is still useful diagnostically,
    # but it is explicitly marked as below the publication target.
    dates=[x[0] for x in run];n=len(dates)
    if n < 3:
        raise RuntimeError('Longest contiguous run has fewer than 3 eligible days; cannot form chronological train/calibration/test splits.')
    ntrain=max(1,int(n*float(cfg['train_fraction'])));ncal=max(1,int(n*float(cfg['calibration_fraction'])))
    if ntrain+ncal>=n:ncal=max(1,n-ntrain-1)
    test_start=ntrain+ncal
    if test_start>=n: raise RuntimeError('Too few days for chronological train/calibration/test split.')
    rec=[]
    for i,d in enumerate(dates):
        split='train' if i<ntrain else ('calibration' if i<test_start else 'test')
        rec.append((d.isoformat(),split))
    write_csv_rows(audit/'split_days.csv',['event_date','split'],[{'event_date':d,'split':s} for d,s in rec])
    man={"start_date":dates[0].isoformat(),"end_date":dates[-1].isoformat(),"days":n,
         "selection":"longest_contiguous_good_day_run","min_daily_rows":minrows,
         "min_experiment_days_target":target_days,"below_target_days":below_target,
         "publication_period_ok":not below_target,
         "train_days":ntrain,"calibration_days":ncal,"test_days":n-test_start,
         "train_end":dates[ntrain-1].isoformat(),"calibration_start":dates[ntrain].isoformat(),"calibration_end":dates[test_start-1].isoformat(),"test_start":dates[test_start].isoformat()}
    atomic_json(audit/'selected_period.json',man)

def main():
    p=argparse.ArgumentParser();p.add_argument('output');p.add_argument('--config');p.add_argument('--memory-limit',default='8GB');p.add_argument('--threads',type=int,default=4);a=p.parse_args();run(a.output,merge_config(a.config),a.memory_limit,a.threads)
if __name__=='__main__':main()
