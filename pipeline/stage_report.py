from __future__ import annotations
import argparse, json, shutil, zipfile
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from common import *

def run(out,cfg):
    out=Path(out).resolve();rep=out/'report';fig=rep/'figures';fig.mkdir(parents=True,exist_ok=True)
    curves=pd.read_csv(out/'evaluation'/'test_delay_curves.csv');met=pd.read_csv(out/'evaluation'/'policy_metrics.csv')
    # Figure 1: test fixed-delay error curves.
    for q,g in curves.groupby('query'):
        plt.figure(figsize=(7.2,4.4));plt.plot(g.delay_seconds/60,g.macro_mean_relative_error,marker='o');plt.xlabel('Finalization delay [min]');plt.ylabel('Macro mean relative error');plt.title(q.replace('_',' '));plt.grid(True,alpha=.25);plt.tight_layout();plt.savefig(fig/f'delay_error_{q}.png',dpi=220);plt.close()
    # Figure 2: policy latency-risk tradeoff, one point per epsilon/alpha/policy.
    mm=met[met.policy!='oracle_offline'].copy();
    plt.figure(figsize=(7.2,4.8))
    for name,g in mm.groupby('policy'):
        plt.scatter(g.mean_delay_seconds/60,g.violation_fraction,label=name)
    plt.xlabel('Mean selected delay [min]');plt.ylabel('Violation fraction');plt.legend();plt.grid(True,alpha=.25);plt.tight_layout();plt.savefig(fig/'policy_latency_risk.png',dpi=220);plt.close()
    audit=load_json(out/'audit'/'global_profile.json');period=load_json(out/'audit'/'selected_period.json')
    best=met.sort_values(['epsilon','alpha','policy'])
    period_ok=period.get('publication_period_ok', True)
    status_line=('Publication-period check: PASS' if period_ok else 'Publication-period check: DIAGNOSTIC ONLY — contiguous period shorter than configured target')
    lines=['# TideMark / StableCQ — local run summary','',f"Rows in typed corpus: {audit.get('rows'):,}",f"Event range: {audit.get('event_min')} — {audit.get('event_max')}",f"Selected experimental period: {period.get('start_date')} — {period.get('end_date')} ({period.get('days')} contiguous days)",status_line,'','## Policies','',best.to_markdown(index=False),'','## Interpretation guardrails','','- `ProcessedDate` is treated as an availability proxy. This is not claimed to be verified network reception time.','- `oracle_offline` is an unattainable lower bound and must not be described as an online baseline.','- Chronological train/calibration/test splits are restricted to one contiguous calendar run; disconnected periods are never merged.','- A result is publication-ready only after inspecting audit anomalies, split coverage, confidence intervals, and source timestamp semantics.']
    (rep/'SUMMARY.md').write_text('\n'.join(lines),encoding='utf-8')
    # Return package excludes raw/core/snapshots/spill; it should remain reasonably small.
    ret=out/'TideMark_RETURN.zip'
    include=[out/'audit',out/'evaluation',out/'manifests',out/'logs',rep,out/'run_manifest.json',out/'config_used.json']
    with zipfile.ZipFile(ret,'w',compression=zipfile.ZIP_DEFLATED) as z:
        for item in include:
            if not item.exists(): continue
            if item.is_file(): z.write(item,item.relative_to(out))
            else:
                for p in sorted(item.rglob('*')):
                    if p.is_file(): z.write(p,p.relative_to(out))
    mark_success(out,'report',{"return_zip":str(ret)})
    print('DONE:',ret)
    return ret

def main():
    p=argparse.ArgumentParser();p.add_argument('output');p.add_argument('--config');a=p.parse_args();run(a.output,merge_config(a.config))
if __name__=='__main__':main()
