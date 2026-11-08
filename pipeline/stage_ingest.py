from __future__ import annotations
import argparse, csv, json, time
from pathlib import Path
from common import *

def run(csv_path, out, cfg, memory_limit, threads, delimiter=None, encoding='utf-8-sig', quick_rows=0):
    src=Path(csv_path).resolve();out=Path(out).resolve()
    sep,names=sniff_csv(src,encoding,delimiter)
    core=out/'core_parquet'
    if core.exists() and any(core.iterdir()):
        raise FileExistsError(f'{core} is not empty. Delete it or use a new output folder.')
    core.mkdir(parents=True,exist_ok=True)
    con=connect_duckdb(out,memory_limit,threads)
    started=time.perf_counter()
    try:
        limit = f" LIMIT {int(quick_rows)}" if quick_rows else ""
        src_sql=sql_str(str(src))
        sep_sql=sql_str(sep)
        # row_number() OVER () is used only to preserve export provenance; no ordering claim is made.
        base=f"""
        WITH src AS (
          SELECT row_number() OVER ()::BIGINT AS export_row_id, *
          FROM read_csv({src_sql}, delim={sep_sql}, header=true, all_varchar=true,
                        parallel=false, strict_mode=true, null_padding=false,
                        store_rejects=true, rejects_scan='csv_reject_scans',
                        rejects_table='csv_reject_errors', rejects_limit=0){limit}
        ), typed AS (
          SELECT
            export_row_id,
            CAST(floor((export_row_id-1)/250000) AS BIGINT) AS export_block_id,
            trim(MovementID) AS movement_id,
            trim(LRIMOShipNo) AS imo,
            trim(MMSI) AS mmsi,
            trim(ShipType) AS ship_type,
            try_cast(MovementDateTime AS TIMESTAMP) AS event_ts,
            try_cast(ProcessedDate AS TIMESTAMP) AS processed_ts,
            try_cast(replace(Latitude, ',', '.') AS DOUBLE) AS lat,
            try_cast(replace(Longitude, ',', '.') AS DOUBLE) AS lon,
            try_cast(replace(Speed, ',', '.') AS DOUBLE) AS speed,
            try_cast(replace(Heading, ',', '.') AS DOUBLE) AS heading,
            nullif(trim(Destination),'') AS destination,
            nullif(trim(ETA),'') AS eta_raw
          FROM src
        )
        SELECT *,
          CAST(event_ts AS DATE) AS event_date,
          coalesce(year(event_ts)::INTEGER,-1) AS event_year,
          coalesce(month(event_ts)::INTEGER,-1) AS event_month,
          CASE WHEN event_ts IS NOT NULL AND processed_ts IS NOT NULL
               THEN epoch(processed_ts)-epoch(event_ts) END AS delay_seconds,
          (event_ts IS NOT NULL AND processed_ts IS NOT NULL) AS valid_time,
          (lat IS NOT NULL AND lon IS NOT NULL AND isfinite(lat) AND isfinite(lon)
             AND lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180) AS valid_position,
          (mmsi IS NOT NULL AND regexp_full_match(mmsi,'[0-9]{{9}}')) AS valid_mmsi,
          (speed IS NOT NULL AND isfinite(speed) AND speed >= 0 AND speed <= {float(cfg['max_valid_speed_knots'])}) AS valid_speed,
          (processed_ts IS NOT NULL AND event_ts IS NOT NULL AND processed_ts >= event_ts) AS nonnegative_delay
        FROM typed
        """
        comp=str(cfg['parquet_compression']).upper()
        rgs=int(cfg['row_group_size'])
        if cfg.get('partition_by_month',True):
            sql=f"COPY ({base}) TO {sql_str(str(core))} (FORMAT PARQUET, COMPRESSION {comp}, ROW_GROUP_SIZE {rgs}, PARTITION_BY (event_year,event_month))"
        else:
            sql=f"COPY ({base}) TO {sql_str(str(core/'core.parquet'))} (FORMAT PARQUET, COMPRESSION {comp}, ROW_GROUP_SIZE {rgs})"
        print('Ingesting CSV -> typed Parquet...',flush=True)
        con.execute(sql)
        rows=con.execute(f"SELECT count(*) FROM read_parquet({sql_str(str(core/'**/*.parquet'))}, hive_partitioning=true)").fetchone()[0]

        # Persist every structurally rejected CSV line. We never silently discard malformed input.
        reject_dir=out/'rejects'; reject_dir.mkdir(parents=True,exist_ok=True)
        reject_errors=0; rejected_lines=0; reject_types=[]
        has_rejects=con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='csv_reject_errors'").fetchone()[0] > 0
        if has_rejects:
            reject_errors=int(con.execute("SELECT count(*) FROM csv_reject_errors").fetchone()[0])
            rejected_lines=int(con.execute("SELECT count(*) FROM (SELECT DISTINCT file_id,line FROM csv_reject_errors)").fetchone()[0])
            reject_types=[{"error_type":str(r[0]),"count":int(r[1])} for r in con.execute("SELECT error_type,count(*) FROM csv_reject_errors GROUP BY 1 ORDER BY 2 DESC,1").fetchall()]
            if reject_errors:
                con.execute(f"COPY (SELECT * FROM csv_reject_errors ORDER BY file_id,line,column_idx) TO {sql_str(str(reject_dir/'csv_reject_errors.parquet'))} (FORMAT PARQUET, COMPRESSION ZSTD)")
                con.execute(f"COPY (SELECT * FROM csv_reject_scans ORDER BY scan_id,file_id) TO {sql_str(str(reject_dir/'csv_reject_scans.parquet'))} (FORMAT PARQUET, COMPRESSION ZSTD)")
                # Small human-readable sample for rapid inspection. Full rejects remain in Parquet.
                sample=con.execute("SELECT line,error_type,column_name,csv_line,error_message FROM csv_reject_errors ORDER BY line,column_idx LIMIT 200").fetchall()
                with (reject_dir/'reject_sample.csv').open('w',newline='',encoding='utf-8-sig') as f:
                    w=csv.writer(f); w.writerow(['line','error_type','column_name','csv_line','error_message']); w.writerows(sample)

        total_physical_records=int(rows)+int(rejected_lines)
        reject_rate=(float(rejected_lines)/total_physical_records) if total_physical_records else 0.0
        man={"status":"complete","rows":int(rows),"elapsed_seconds":time.perf_counter()-started,
             "delimiter":sep,"columns":names,"expected_schema_match":names==EXPECTED_COLUMNS,
             "quick_rows":int(quick_rows),"core_parquet":str(core),"input_signature":file_signature(src),
             "csv_reject_error_records":reject_errors,"csv_rejected_lines":rejected_lines,
             "csv_reject_rate":reject_rate,"csv_reject_types":reject_types,
             "csv_rejects_note":"Structurally malformed source lines were excluded from typed Parquet and preserved verbatim in rejects/csv_reject_errors.parquet. They were not repaired or silently ignored."}
        atomic_json(out/'manifests'/'ingest.json',man)
        mark_success(out,'ingest',{"rows":int(rows)})
        print(json.dumps(man,indent=2),flush=True)
        return man
    finally:
        con.close()

def main():
    p=argparse.ArgumentParser();p.add_argument('csv');p.add_argument('output');p.add_argument('--config');p.add_argument('--memory-limit',default='8GB');p.add_argument('--threads',type=int,default=4);p.add_argument('--delimiter');p.add_argument('--encoding',default='utf-8-sig');p.add_argument('--quick-rows',type=int,default=0)
    a=p.parse_args();run(a.csv,a.output,merge_config(a.config),a.memory_limit,a.threads,a.delimiter,a.encoding,a.quick_rows)
if __name__=='__main__':main()
