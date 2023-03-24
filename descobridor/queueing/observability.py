from typing import Optional, Tuple
import pandas as pd
from datetime import datetime
from descobridor.queueing.constants import CURRENT_VPN_SUFFIX

from truby.db_connection import RedisConnection


def see_redis_vpn_state(do_print=True) -> Optional[Tuple[dict, str]]:
    with RedisConnection() as r:
        all_vpns = r.connection.hgetall("vpns")
        keys = r.connection.scan_iter(f"*_{CURRENT_VPN_SUFFIX}")
        current_vpns = [(k, r.connection.get(k), r.connection.ttl(k)) for k in keys]
        
    all_vpns = pd.DataFrame.from_dict(
        {k.decode("utf-8"): datetime.fromtimestamp(float(v.decode("utf-8"))) 
         for k, v in all_vpns.items()},
        orient="index", columns=["last_used"]
        ).sort_values("last_used", ascending=False)
    
    
    current_vpns = pd.DataFrame.from_records(
        [
            (k.decode("utf-8"), v.decode("utf-8"), ttl)
            for k, v, ttl in current_vpns
            ], columns=["worker", "vpn", "ttl"])
        
        
    if do_print:
        print(all_vpns)
        print(current_vpns)
    else:
        return all_vpns, current_vpns
