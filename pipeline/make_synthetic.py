#!/usr/bin/env python3
import argparse,csv,datetime as dt,math,random
from pathlib import Path
from common import EXPECTED_COLUMNS

def main():
    p=argparse.ArgumentParser();p.add_argument('output');p.add_argument('--rows',type=int,default=50000);p.add_argument('--seed',type=int,default=42);a=p.parse_args();rng=random.Random(a.seed);path=Path(a.output);path.parent.mkdir(parents=True,exist_ok=True)
    t0=dt.datetime(2025,1,1)
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=EXPECTED_COLUMNS);w.writeheader()
        for i in range(a.rows):
            e=t0+dt.timedelta(seconds=30*i)
            # Diurnal/congestion-like regimes create context-dependent delays.
            hour=e.hour;base=25 if 6<=hour<18 else 80
            late=rng.random()<(.08 if 6<=hour<18 else .20)
            delay=max(0,rng.expovariate(1/base)+(rng.uniform(300,2400) if late else 0))
            mmsi=str(200000000+(i%4000))
            speed=max(0,min(35,rng.gauss(11+(i%5),4)))
            row={c:'' for c in EXPECTED_COLUMNS};row.update(MovementID=str(i+1),LRIMOShipNo=str(9000000+i%1000),MMSI=mmsi,ShipName=f'V{i%4000}',ShipType='Cargo',CallSign=f'C{i%10000}',MovementDateTime=e.isoformat(sep=' '),Latitude=f'{54.0+(i%80)*.01:.5f}',Longitude=f'{14.0+(i%120)*.01:.5f}',MoveStatus='0',Heading=str(i%360),Draught='6.5',Speed=f'{speed:.2f}',Destination='PORT',ETA=(e+dt.timedelta(hours=6)).isoformat(sep=' '),Length='120',Beam='20',AdditionalInfo='',ProcessedDate=(e+dt.timedelta(seconds=delay)).isoformat(sep=' '));w.writerow(row)
    print(path)
if __name__=='__main__':main()
