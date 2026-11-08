from __future__ import annotations
import csv, hashlib, json, os, platform, shutil, sys, time
from pathlib import Path

EXPECTED_COLUMNS = [
    'MovementID','LRIMOShipNo','MMSI','ShipName','ShipType','CallSign',
    'MovementDateTime','Latitude','Longitude','MoveStatus','Heading','Draught',
    'Speed','Destination','ETA','Length','Beam','AdditionalInfo','ProcessedDate'
]
REQUIRED_COLUMNS = ['MovementID','LRIMOShipNo','MMSI','ShipType','MovementDateTime','Latitude','Longitude','Heading','Speed','Destination','ETA','ProcessedDate']

DEFAULT_CONFIG = {
    "window_seconds": 900,
    "cell_degrees": 0.25,
    "candidate_delays_seconds": [0,30,60,120,300,600,900,1200,1800,3600,7200,14400,28800,86400],
    "epsilons": [0.0,0.01,0.05],
    "alphas": [0.01,0.05],
    "train_fraction": 0.60,
    "calibration_fraction": 0.20,
    "min_daily_rows": 1000,
    "min_experiment_days": 30,
    "activity_bins": 4,
    "min_context_windows": 200,
    "speed_threshold_knots": 12.0,
    "max_valid_speed_knots": 70.0,
    "parquet_compression": "zstd",
    "row_group_size": 122880,
    "partition_by_month": True,
    "bootstrap_replicates": 1000,
    "bootstrap_seed": 20260929,
    "exact_movement_id_repetition": False
}

def now_iso():
    import datetime as _dt
    return _dt.datetime.now().astimezone().isoformat(timespec='seconds')

def atomic_json(path: Path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')
    tmp.replace(path)

def load_json(path: Path):
    return json.loads(Path(path).read_text(encoding='utf-8'))

def merge_config(config_path: str | None):
    cfg = dict(DEFAULT_CONFIG)
    if config_path:
        user = load_json(Path(config_path))
        unknown = sorted(set(user) - set(cfg))
        if unknown:
            raise ValueError(f"Unknown config keys: {unknown}")
        cfg.update(user)
    ds = [int(x) for x in cfg['candidate_delays_seconds']]
    if ds != sorted(set(ds)) or not ds or ds[0] < 0:
        raise ValueError('candidate_delays_seconds must be unique, sorted, nonnegative')
    cfg['candidate_delays_seconds'] = ds
    for key in ('epsilons','alphas'):
        vals=[float(x) for x in cfg[key]]
        if not vals or any(x < 0 or x > 1 for x in vals):
            raise ValueError(f'{key} values must be in [0,1]')
        cfg[key]=vals
    if not (0 < cfg['train_fraction'] < 1 and 0 < cfg['calibration_fraction'] < 1 and
            cfg['train_fraction'] + cfg['calibration_fraction'] < 1):
        raise ValueError('Invalid chronological split fractions')
    return cfg

def sniff_csv(path: Path, encoding='utf-8-sig', delimiter=None):
    path=Path(path)
    with path.open('r', encoding=encoding, newline='', errors='strict') as f:
        header=f.readline()
    if not header:
        raise ValueError('Empty CSV')
    sep = delimiter or csv.Sniffer().sniff(header, delimiters=',;\t|').delimiter
    names=next(csv.reader([header], delimiter=sep))
    names=[x.strip() for x in names]
    if len(names) != len(set(names)):
        raise ValueError('Duplicate header names')
    missing=[x for x in REQUIRED_COLUMNS if x not in names]
    if missing:
        raise ValueError('Missing required columns: '+', '.join(missing))
    return sep,names

def prefix_sha256(path: Path, n=1048576):
    with Path(path).open('rb') as f:
        return hashlib.sha256(f.read(n)).hexdigest()

def file_signature(path: Path):
    st=Path(path).stat()
    return {"name":Path(path).name,"bytes":st.st_size,"mtime_ns":st.st_mtime_ns,
            "first_MiB_sha256":prefix_sha256(path)}

def dir_free_bytes(path: Path):
    path=Path(path)
    path.mkdir(parents=True,exist_ok=True)
    return shutil.disk_usage(path).free

def human_bytes(n):
    n=float(n)
    units=['B','KiB','MiB','GiB','TiB']
    for u in units:
        if abs(n)<1024 or u==units[-1]: return f'{n:.2f} {u}'
        n/=1024

def success_path(out: Path, stage: str):
    return Path(out)/'_stages'/f'{stage}.json'

def mark_success(out: Path, stage: str, payload=None):
    p=success_path(out,stage)
    p.parent.mkdir(parents=True,exist_ok=True)
    data={"stage":stage,"status":"complete","completed_at":now_iso()}
    if payload:data.update(payload)
    atomic_json(p,data)

def is_success(out: Path, stage: str):
    p=success_path(out,stage)
    if not p.exists(): return False
    try:return load_json(p).get('status')=='complete'
    except Exception:return False

def stage_log(out: Path, stage: str):
    p=Path(out)/'logs'/f'{stage}.log'; p.parent.mkdir(parents=True,exist_ok=True); return p

def connect_duckdb(out: Path, memory_limit: str, threads: int):
    import duckdb
    con=duckdb.connect()
    con.execute("SET memory_limit = ?", [memory_limit])
    con.execute("SET threads = ?", [int(threads)])
    con.execute("SET preserve_insertion_order = true")
    temp=Path(out)/'spill'; temp.mkdir(parents=True,exist_ok=True)
    con.execute("SET temp_directory = ?", [str(temp.resolve())])
    return con

def qpath(path: Path):
    return str(Path(path).resolve()).replace("'", "''")

def sql_str(s: str):
    return "'"+str(s).replace("'","''")+"'"

def write_csv_rows(path: Path, fieldnames, rows):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=fieldnames);w.writeheader();w.writerows(rows)

def system_manifest():
    d={"python":platform.python_version(),"platform":platform.platform(),"processor":platform.processor(),
       "logical_cpus":os.cpu_count()}
    try:
        import duckdb; d['duckdb']=duckdb.__version__
    except Exception: d['duckdb']=None
    try:
        import pandas; d['pandas']=pandas.__version__
    except Exception: d['pandas']=None
    try:
        import numpy; d['numpy']=numpy.__version__
    except Exception: d['numpy']=None
    return d
