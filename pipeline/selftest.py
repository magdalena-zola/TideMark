#!/usr/bin/env python3
import shutil,subprocess,sys,tempfile,traceback
from pathlib import Path

def main():
    root=Path(__file__).resolve().parent
    td=Path(tempfile.mkdtemp(prefix='tidemark_selftest_'))
    csv=td/'synthetic.csv';out=td/'out'
    try:
        subprocess.run([sys.executable,str(root/'make_synthetic.py'),str(csv),'--rows','50000'],check=True)
        subprocess.run([sys.executable,str(root/'run_pipeline.py'),str(csv),str(out),'--preset','low'],check=True)
        must=[out/'TideMark_RETURN.zip',out/'audit'/'global_profile.json',out/'evaluation'/'policy_metrics.csv',out/'report'/'SUMMARY.md']
        missing=[str(p) for p in must if not p.exists()]
        snapshot_files=list((out/'snapshots'/'data').rglob('*.parquet')) if (out/'snapshots'/'data').exists() else []
        if not snapshot_files: missing.append(str(out/'snapshots'/'data'/'**'/'*.parquet'))
        if missing: raise RuntimeError('Missing outputs: '+repr(missing))
        print('SELFTEST OK')
        shutil.rmtree(td,ignore_errors=True)
    except Exception:
        print('SELFTEST FAILED; diagnostic directory preserved at:',td,file=sys.stderr)
        for log in sorted((out/'logs').glob('*.log')) if (out/'logs').exists() else []:
            print('\n===== '+str(log)+' =====',file=sys.stderr)
            try:
                lines=log.read_text(encoding='utf-8',errors='replace').splitlines()
                for line in lines[-80:]: print(line,file=sys.stderr)
            except Exception as e: print(repr(e),file=sys.stderr)
        raise

if __name__=='__main__': main()
