from typing import Tuple
import os
from datetime import datetime
import random

from truby.db_connection import RedisConnection
from descobridor.queueing.constants import (
    VPN_COUNTRIES
)


def get_vpns(list_of_countries: Tuple[str, ...]):
    """get list of openvpn config files for the given countries"""
    vpns = [ f for f in
        os.listdir(os.environ["OPENVPN_CONFIGS_DIR"])
        if (f.startswith(tuple(list_of_countries))
            and f != "secrets")
    ]
    random.Random(10).shuffle(vpns)
    vpns = [
        (vpns[i], round(i/len(vpns)*24,1), datetime(2023,1,1,0,0,0).timestamp())
        for i in range(len(vpns))
    ]
    return vpns


def vpns_to_redis(vpns: list):
    """add vpns to redis"""
    with RedisConnection() as r:
        r.connection.delete("vpns")
        for vpn in vpns:
            r.connection.hset("vpns", f"{vpn[0]}_{vpn[1]}", vpn[2])
    return True


def main():
    vpns = get_vpns(VPN_COUNTRIES)
    vpns_to_redis(vpns)
    return True

if __name__ == "__main__":
    main()
    
