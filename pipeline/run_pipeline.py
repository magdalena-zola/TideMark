#!/usr/bin/env python3
from __future__ import annotations
import argparse, contextlib, json, os, subprocess, sys, traceback
from pathlib import Path
from common import *

def choose_resources(preset, memory, threads):
    presets={'low':('4GB',2),'standard':('8GB',4),'high':('24GB',8)}
    m,t=presets[preset]
    return memory or m, threads or t

def run_sub(stage,script,args,out):
    log=stage_log(out,stage);print(f'[{stage}] log: {log}',flush=True)
    cmd=[sys.executable,str(Path(__file__).resolve().parent/script)]+[str(x) for x in args]
    with log.open('w',encoding='utf-8') as f:
        f.write('COMMAND: '+' '.join(cmd)+'\n\n');f.flush()
        proc=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
    if proc.returncode!=0:
        print(f'[{stage}] FAILED. Last log lines:',file=sys.stderr)
        try:
            lines=log.read_text(encoding='utf-8',errors='replace').splitlines()
            for line in lines[-80:]: print(line,file=sys.stderr)
        except Exception:
            print(f'Inspect {log}',file=sys.stderr)
        raise SystemExit(proc.returncode)
    print(f'[{stage}] complete',flush=True)


def cleanup_from(out, stage):
    import shutil
    order=['ingest','audit','snapshots','evaluate','report']
    roots={
      'ingest':['core_parquet','audit','snapshots','evaluation','report','TideMark_RETURN.zip'],
      'audit':['audit','snapshots','evaluation','report','TideMark_RETURN.zip'],
      'snapshots':['snapshots','evaluation','report','TideMark_RETURN.zip'],
      'evaluate':['evaluation','report','TideMark_RETURN.zip'],
      'report':['report','TideMark_RETURN.zip']}
    for rel in roots[stage]:
        q=Path(out)/rel
        if q.is_dir(): shutil.rmtree(q)
        elif q.exists(): q.unlink()
    i=order.index(stage)
    for st in order[i:]:
        m=success_path(out,st)
        if m.exists():m.unlink()

def main():
    p=argparse.ArgumentParser(description='Resumable local pipeline for TideMark / StableCQ ICDE experiments.')
    p.add_argument('csv');p.add_argument('output');p.add_argument('--config');p.add_argument('--preset',choices=['low','standard','high'],default='standard');p.add_argument('--memory-limit');p.add_argument('--threads',type=int);p.add_argument('--delimiter');p.add_argument('--encoding',default='utf-8-sig');p.add_argument('--quick-rows',type=int,default=0,help='Physical CSV prefix for smoke tests only; 0=full file');p.add_argument('--restart-stage',choices=['ingest','audit','snapshots','evaluate','report']);p.add_argument('--stop-after',choices=['ingest','audit','snapshots','evaluate','report']);a=p.parse_args()
    src=Path(a.csv).resolve();out=Path(a.output).resolve();
    if not src.is_file():p.error('CSV not found')
    out.mkdir(parents=True,exist_ok=True)
    cfg=merge_config(a.config);atomic_json(out/'config_used.json',cfg)
    mem,threads=choose_resources(a.preset,a.memory_limit,a.threads)
    sep,names=sniff_csv(src,a.encoding,a.delimiter)
    pre={"created_at":now_iso(),"input":file_signature(src),"header_columns":names,"delimiter":sep,"expected_schema_match":names==EXPECTED_COLUMNS,"system":system_manifest(),"memory_limit":mem,"threads":threads,"preset":a.preset,"quick_rows":a.quick_rows,"free_bytes_at_start":dir_free_bytes(out)}
    atomic_json(out/'run_manifest.json',pre)
    print('Input:',src);print('Size:',human_bytes(src.stat().st_size));print('Output:',out);print('Free disk:',human_bytes(pre['free_bytes_at_start']));print('DuckDB memory limit:',mem,'threads:',threads)
    if pre['free_bytes_at_start'] < src.stat().st_size:
        print('WARNING: free space is smaller than input size. Parquet is compressed, but snapshots/spill may still exhaust the disk; prefer a larger SSD/NVMe target.')
    if names!=EXPECTED_COLUMNS:print('WARNING: header differs from expected 19-column export; required columns are present, so the pipeline will continue.')
    if a.restart_stage:
        print(f'Cleaning outputs from stage {a.restart_stage} onward...',flush=True);cleanup_from(out,a.restart_stage)
    stages=[('ingest','stage_ingest.py',[src,out,'--memory-limit',mem,'--threads',threads,'--encoding',a.encoding,'--quick-rows',a.quick_rows]),('audit','stage_audit.py',[out,'--memory-limit',mem,'--threads',threads]),('snapshots','stage_snapshots.py',[out,'--memory-limit',mem,'--threads',threads]),('evaluate','stage_evaluate.py',[out,'--memory-limit',mem,'--threads',threads]),('report','stage_report.py',[out])]
    if a.config:
        for s in stages:s[2].extend(['--config',str(Path(a.config).resolve())])
    if a.delimiter:stages[0][2].extend(['--delimiter',a.delimiter])
    for name,script,args in stages:
        if is_success(out,name):print(f'[{name}] already complete; skipping',flush=True)
        else:run_sub(name,script,args,out)
        if a.stop_after==name:break
    if is_success(out,'report'):print('Return only:',out/'TideMark_RETURN.zip')

if __name__=='__main__':main()
