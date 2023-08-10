import glob
import pandas as pd
from datetime import datetime,timedelta
import numpy as np
pd.set_option('display.max_columns', None)

list_call_log = glob.glob(r'D:\rerun_5\data\pdt\alo2_call*.pq')
list_event = glob.glob(r'D:\rerun_5\data\pdt\deli_*.pq')

raw = {}
for i in list_event:
    for a in list_call_log:
        if i[31:33] == a[34:36]:
            raw[i] = a

a = 0
b = 0 

for event,call_log in raw.items():
    print(event)
    print(call_log)
    d = pd.read_parquet(event)
    c = pd.read_parquet(call_log)
    
    d = d[['driver_id','route_id','callee','attempt_datetime']]
    d.attempt_datetime = d.attempt_datetime.str[:19].astype('datetime64[ns]')
    d['callee'] = d['callee'].str.replace(' ','').str[-9:]
    d = d.groupby(['driver_id','route_id','callee'], as_index = False).agg({'attempt_datetime':'max'})
    d.sort_values(by =  'attempt_datetime', ascending = True, inplace = True)
    d['last_attempt_datetime'] = d.groupby('callee').attempt_datetime.shift(1)

    full = d.merge(c, on = 'callee', how = 'left')
    full = full[full.attempt_datetime >= full.started_at]
    full.drop(full[(full.last_attempt_datetime.isna()== False) & (full.started_at < full.last_attempt_datetime)].index, inplace = True)

    full['att_second'] = (full.attempt_datetime.dt.hour)*3600 + (full.attempt_datetime.dt.minute)*60 + (full.attempt_datetime.dt.second)
    full['sync_second'] = (full.sync_at.dt.hour)*3600 + (full.sync_at.dt.minute)*60 + (full.sync_at.dt.second)
    full = full[['att_second','sync_second']]
    full['sync_diff'] = full['att_second'] - full['sync_second']
    full['is_late'] = full.apply(lambda x: 1 if x.sync_diff < 0 else 0, axis = 1)
    full = full[full.is_late == 1]
    full.sync_diff = (full.sync_diff)*(-1)
    a += full.sync_diff.sum()
    b += full.sync_diff.count()

result = a/b
print(result)
