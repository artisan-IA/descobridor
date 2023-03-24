from typing import Optional, Tuple
import pandas as pd
from datetime import datetime

from truby.db_connection import RedisConnection


def see_redis(do_print=True) -> Optional[Tuple[dict, str]]:
    with RedisConnection() as r:
        all_vpns = r.connection.hgetall("vpns")
        current_vpn = r.connection.get("current_vpn")
        
    all_vpns = pd.DataFrame.from_dict(
        {k.decode("utf-8"): datetime.fromtimestamp(float(v.decode("utf-8"))) 
         for k, v in all_vpns.items()},
        orient="index", columns=["last_used"]
        ).sort_values("last_used")
        
    if do_print:
        print(all_vpns)
        print(current_vpn)
    else:
        return all_vpns, current_vpn
