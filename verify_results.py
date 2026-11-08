#!/usr/bin/env python3
"""Independent, standard-library-only trace check for the six TideMark returns.
Does not replay AIS raw input; validates released derived metrics.
"""
from pathlib import Path
import csv, json, math, sys, hashlib
ROOT = Path(__file__).resolve().parent
VARIANTS = ['w10_o0','w15_o0','w15_o5','w15_o10','w30_o0','w60_o0']

def read_csv(p):
    with p.open(newline='',encoding='utf-8') as f:return list(csv.DictReader(f))

def close(a,b):
    return math.isclose(a,b,rel_tol=1e-10,abs_tol=1e-12)

def main():
    results = ROOT/'results'/'sensitivity'
    for variant in VARIANTS:
        p=results/variant
        manifest=json.loads((p/'manifest.json').read_text(encoding='utf-8'))
        assert manifest.get('status') == 'complete', (variant,'manifest')
        overall=read_csv(p/'policy_metrics.csv'); by_query=read_csv(p/'policy_metrics_by_query.csv')
        assert len(overall)==24 and len(by_query)==72,(variant,len(overall),len(by_query))
        for r in overall:
            sub=[x for x in by_query if x['policy']==r['policy'] and x['epsilon']==r['epsilon'] and x['alpha']==r['alpha']]
            assert len(sub)==3,(variant,r)
            n=sum(int(x['windows']) for x in sub)
            assert n==int(r['windows']),(variant,'instances',r['policy'])
            for key in ['mean_delay_seconds','violation_fraction']:
                weighted=sum(int(x['windows'])*float(x[key]) for x in sub)/n
                assert close(weighted,float(r[key])),(variant,key,r['policy'],weighted,r[key])
        rr=[r for r in overall if r['epsilon']=='0.01' and r['alpha']=='0.01']
        t=next(x for x in rr if x['policy']=='tidemark_context')
        g=next(x for x in rr if x['policy']=='fixed_global')
        pct=(1-float(t['mean_delay_seconds'])/float(g['mean_delay_seconds']))*100 if float(g['mean_delay_seconds'])>0 else None
        print(f"PASS {variant}: n={t['windows']}, delay={float(t['mean_delay_seconds'])/60:.3f} min, risk={100*float(t['violation_fraction']):.4f}%, gain={pct}")
    print('PASS six variants; derived-results verification ONLY (not raw-data reproduction)')

if __name__=='__main__':main()
