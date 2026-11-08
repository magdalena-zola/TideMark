from __future__ import annotations
import argparse, json, math, time
from pathlib import Path
import numpy as np
import pandas as pd
from common import *

def err_expr(d):
    d=int(d);return f"(final_value-o_{d})*1.0/final_value"

def selected_error_expr(delays, chosen='chosen_delay'):
    parts=' '.join([f"WHEN {int(d)} THEN {err_expr(d)}" for d in delays])
    return f"CASE {chosen} {parts} ELSE {err_expr(max(delays))} END"

def activity_case(edges):
    outer=[]
    for q,qs in edges.items():
        cond=[]
        for i,e in enumerate(qs):cond.append(f"WHEN o_0 <= {float(e)} THEN {i}")
        cond.append(f"ELSE {len(qs)}")
        outer.append(f"WHEN query={sql_str(q)} THEN CASE {' '.join(cond)} END")
    return "CASE "+' '.join(outer)+" ELSE 0 END"

def choose_from_summary(df,delays,alpha):
    good=df[df.risk<=alpha].sort_values('delay_seconds')
    return int(good.iloc[0].delay_seconds) if len(good) else int(max(delays))

def build_rules(con,delays,eps,alpha,min_context):
    specs=[(['query','hour_of_day','activity_bin'],'q_hour_activity'),(['query','activity_bin'],'q_activity'),(['query'],'query'),([], 'global')]
    rules=[]
    for keys,level in specs:
        selects=[]
        for d in delays:
            ksel=(','.join(keys)+',') if keys else ''
            kgroup=(' GROUP BY '+','.join(keys)) if keys else ''
            selects.append(f"SELECT {ksel}{int(d)}::BIGINT delay_seconds,count(*)::BIGINT windows,avg(CASE WHEN {err_expr(d)}>{float(eps)} THEN 1.0 ELSE 0.0 END) risk FROM snap_enriched WHERE split='calibration'{kgroup}")
        s=con.execute(' UNION ALL '.join(selects)).fetchdf()
        if keys:
            grouped=s.groupby(keys,dropna=False,sort=False)
            for key,g in grouped:
                if not isinstance(key,tuple):key=(key,)
                n=int(g['windows'].max())
                if level not in ('query','global') and n<int(min_context):continue
                rec={'level':level,'delay_seconds':choose_from_summary(g,delays,alpha),'calibration_windows':n}
                for k,v in zip(keys,key):rec[k]=int(v) if k in ('hour_of_day','activity_bin') else str(v)
                rules.append(rec)
        else:
            n=int(s['windows'].max());rules.append({'level':level,'delay_seconds':choose_from_summary(s,delays,alpha),'calibration_windows':n})
    return pd.DataFrame(rules)

def make_rule_tables(con,rules):
    # Register each level as a tiny relation. Explicit casts keep join types stable.
    for level,keys in [('q_hour_activity',['query','hour_of_day','activity_bin']),('q_activity',['query','activity_bin']),('query',['query']),('global',[])]:
        x=rules[rules.level==level].copy()
        name='r_'+level
        if x.empty:
            if keys:
                cols=', '.join((["CAST(NULL AS VARCHAR) query"] if 'query' in keys else [])+(["CAST(NULL AS INTEGER) hour_of_day"] if 'hour_of_day' in keys else [])+(["CAST(NULL AS INTEGER) activity_bin"] if 'activity_bin' in keys else [])+["CAST(NULL AS BIGINT) chosen_delay"])
                con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT {cols} WHERE false")
            else:con.execute(f"CREATE OR REPLACE TEMP TABLE {name}(chosen_delay BIGINT)")
            continue
        keep=keys+['delay_seconds'];x=x[keep].rename(columns={'delay_seconds':'chosen_delay'})
        con.register(name+'_df',x);con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM {name}_df")

def chosen_sql(delays,mode):
    maxd=int(max(delays));e=selected_error_expr(delays)
    if mode=='tidemark_context':
        choice=f"coalesce(r1.chosen_delay,r2.chosen_delay,r3.chosen_delay,r4.chosen_delay,{maxd})"
        joins="""LEFT JOIN r_q_hour_activity r1 ON b.query=r1.query AND b.hour_of_day=r1.hour_of_day AND b.activity_bin=r1.activity_bin
        LEFT JOIN r_q_activity r2 ON b.query=r2.query AND b.activity_bin=r2.activity_bin
        LEFT JOIN r_query r3 ON b.query=r3.query
        LEFT JOIN r_global r4 ON true"""
    elif mode=='query_static':
        choice=f"coalesce(r3.chosen_delay,{maxd})";joins="LEFT JOIN r_query r3 ON b.query=r3.query"
    elif mode=='fixed_global':
        choice=f"coalesce(r4.chosen_delay,{maxd})";joins="LEFT JOIN r_global r4 ON true"
    else:raise ValueError(mode)
    return f"""WITH b AS (SELECT * FROM snap_enriched WHERE split='test'), c AS (
      SELECT b.*,{choice}::BIGINT AS chosen_delay FROM b {joins})
      SELECT *,{e} AS selected_error FROM c"""

def aggregate_policy(con,base_sql,eps,policy,alpha):
    # `policy` is a SQL keyword in current DuckDB releases.  Use a neutral
    # SQL alias and rename it back only after the query has been materialized
    # as a small pandas result table.
    q=f"""SELECT {sql_str(policy)} AS policy_name,{float(eps)} AS epsilon,{float(alpha)} AS alpha,count(*)::BIGINT AS windows,
      avg(chosen_delay) AS mean_delay_seconds,quantile_cont(chosen_delay,0.5) AS median_delay_seconds,quantile_cont(chosen_delay,0.95) AS p95_delay_seconds,
      avg(CASE WHEN selected_error>{float(eps)} THEN 1.0 ELSE 0.0 END) AS violation_fraction,
      avg(CASE WHEN selected_error>0 THEN 1.0 ELSE 0.0 END) AS any_change_fraction,
      avg(selected_error) AS macro_mean_relative_error,quantile_cont(selected_error,0.95) AS p95_relative_error FROM ({base_sql}) AS selected_rows"""
    row=con.execute(q).fetchdf().rename(columns={'policy_name':'policy'})
    byq=con.execute(f"""SELECT {sql_str(policy)} AS policy_name,{float(eps)} AS epsilon,{float(alpha)} AS alpha,query,count(*)::BIGINT AS windows,avg(chosen_delay) AS mean_delay_seconds,
      avg(CASE WHEN selected_error>{float(eps)} THEN 1.0 ELSE 0.0 END) AS violation_fraction,avg(selected_error) AS macro_mean_relative_error,
      quantile_cont(selected_error,0.95) AS p95_relative_error FROM ({base_sql}) AS selected_rows GROUP BY query ORDER BY query""").fetchdf().rename(columns={'policy_name':'policy'})
    daily=con.execute(f"""SELECT event_date,{sql_str(policy)} AS policy_name,{float(eps)} AS epsilon,{float(alpha)} AS alpha,
      avg(chosen_delay) AS mean_delay_seconds,avg(CASE WHEN selected_error>{float(eps)} THEN 1.0 ELSE 0.0 END) AS violation_fraction,avg(selected_error) AS macro_mean_relative_error
      FROM ({base_sql}) AS selected_rows GROUP BY event_date ORDER BY event_date""").fetchdf().rename(columns={'policy_name':'policy'})
    return row,byq,daily

def oracle_sql(delays,eps):
    maxd=int(max(delays));terms=[]
    for d in delays:terms.append(f"WHEN {err_expr(d)} <= {float(eps)} THEN {int(d)}")
    choice='CASE '+' '.join(terms)+f' ELSE {maxd} END'
    e=selected_error_expr(delays)
    return f"""WITH c AS (SELECT *,({choice})::BIGINT chosen_delay FROM snap_enriched WHERE split='test') SELECT *,{e} AS selected_error FROM c"""

def bootstrap_daily(daily,reps,seed):
    rows=[];rng=np.random.default_rng(seed)
    for (policy,eps,alpha),g in daily.groupby(['policy','epsilon','alpha'],dropna=False):
        for metric in ['mean_delay_seconds','violation_fraction','macro_mean_relative_error']:
            x=g[metric].to_numpy(float)
            if len(x)<2:continue
            vals=np.array([rng.choice(x,size=len(x),replace=True).mean() for _ in range(reps)])
            rows.append({'policy':policy,'epsilon':eps,'alpha':alpha,'metric':metric,'estimate':float(x.mean()),'ci025':float(np.quantile(vals,.025)),'ci975':float(np.quantile(vals,.975)),'days':len(x)})
    return pd.DataFrame(rows)

def run(out,cfg,memory_limit,threads):
    out=Path(out).resolve();ev=out/'evaluation';ev.mkdir(parents=True,exist_ok=True);started=time.perf_counter();G=sql_str(str(out/'snapshots'/'data'/'**/*.parquet'));D=[int(x) for x in cfg['candidate_delays_seconds']]
    con=connect_duckdb(out,memory_limit,threads)
    try:
        # Activity-bin cut points are derived from train only and then frozen.
        edges={}
        for q in ['message_count','distinct_vessels','high_speed_vessels']:
            vals=con.execute(f"SELECT quantile_cont(o_0,[0.25,0.5,0.75]) FROM read_parquet({G},hive_partitioning=true) WHERE split='train' AND query={sql_str(q)}").fetchone()[0]
            edges[q]=sorted(set(float(x) for x in (vals or [])))
        atomic_json(ev/'activity_bin_edges.json',edges)
        acase=activity_case(edges)
        con.execute(f"CREATE TEMP VIEW snap_enriched AS SELECT *,({acase})::INTEGER activity_bin FROM read_parquet({G},hive_partitioning=true)")
        # Transparent test curves without materializing long snapshots.
        curve_parts=[]
        for d in D:
            cols=[f"query,{int(d)}::BIGINT delay_seconds,count(*)::BIGINT cell_windows,avg({err_expr(d)}) macro_mean_relative_error,quantile_cont({err_expr(d)},0.95) p95_relative_error,avg(CASE WHEN {err_expr(d)}=0 THEN 1.0 ELSE 0.0 END) exact_fraction"]
            for eps in cfg['epsilons']:cols.append(f"avg(CASE WHEN {err_expr(d)}>{float(eps)} THEN 1.0 ELSE 0.0 END) AS violation_eps_{str(float(eps)).replace('.','_')}")
            curve_parts.append(f"SELECT {','.join(cols)} FROM snap_enriched WHERE split='test' GROUP BY query")
        curve=con.execute(' UNION ALL '.join(curve_parts)+' ORDER BY query,delay_seconds').fetchdf();curve.to_csv(ev/'test_delay_curves.csv',index=False)
        metrics=[];byq=[];daily_all=[];rules_all=[]
        for eps in cfg['epsilons']:
          for alpha in cfg['alphas']:
            rules=build_rules(con,D,float(eps),float(alpha),int(cfg['min_context_windows']));rules['epsilon']=float(eps);rules['alpha']=float(alpha);rules_all.append(rules);make_rule_tables(con,rules)
            for mode in ['fixed_global','query_static','tidemark_context']:
                bsql=chosen_sql(D,mode);a,b,d=aggregate_policy(con,bsql,float(eps),mode,float(alpha));metrics.append(a);byq.append(b);daily_all.append(d)
            osql=oracle_sql(D,float(eps));a,b,d=aggregate_policy(con,osql,float(eps),'oracle_offline',float(alpha));metrics.append(a);byq.append(b);daily_all.append(d)
        metrics_df=pd.concat(metrics,ignore_index=True);metrics_df.to_csv(ev/'policy_metrics.csv',index=False)
        pd.concat(byq,ignore_index=True).to_csv(ev/'policy_metrics_by_query.csv',index=False)
        daily=pd.concat(daily_all,ignore_index=True);daily.to_csv(ev/'daily_policy_metrics.csv',index=False)
        if rules_all:pd.concat(rules_all,ignore_index=True).to_csv(ev/'tidemark_rules.csv',index=False)
        boot=bootstrap_daily(daily,int(cfg['bootstrap_replicates']),int(cfg['bootstrap_seed']));boot.to_csv(ev/'bootstrap_day_ci.csv',index=False)
        counts=con.execute("SELECT split,query,count(*) n FROM snap_enriched GROUP BY split,query ORDER BY split,query").fetchdf()
        man={"status":"complete","elapsed_seconds":time.perf_counter()-started,"cell_windows_by_split_query":counts.to_dict(orient='records'),"note":"All heavy evaluation is executed in DuckDB; pandas receives only aggregated tables. oracle_offline is not an online policy."}
        # normalize numpy scalar types before JSON
        man['cell_windows_by_split_query']=[{'split':str(x['split']),'query':str(x['query']),'n':int(x['n'])} for x in man['cell_windows_by_split_query']]
        atomic_json(out/'manifests'/'evaluation.json',man);mark_success(out,'evaluate');print(json.dumps(man,indent=2));return man
    finally:con.close()

def main():
    p=argparse.ArgumentParser();p.add_argument('output');p.add_argument('--config');p.add_argument('--memory-limit',default='8GB');p.add_argument('--threads',type=int,default=4);a=p.parse_args();run(a.output,merge_config(a.config),a.memory_limit,a.threads)
if __name__=='__main__':main()
