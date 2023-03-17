from typing import Tuple
import os
from scipy.stats import norm # noqa F401

from truby.db_connection import RedisConnection
from descobridor.queueing.constants import (
    VPN_COUNTRIES # noqa F401
)


def get_vpns(list_of_countries: Tuple[str, ...]):
    """get list of openvpn config files for the given countries"""
    vpns = sorted([ f for f in
        os.listdir(os.environ["OPENVPN_CONFIGS_DIR"])
        if (f.startswith(tuple(list_of_countries))
            and f != "secrets")
    ])
    vpns = [
        (vpns[i], round(i/len(vpns)*24,1), None)
        for i in range(len(vpns))
    ]
    return vpns


def vpns_to_redis(vpns: list):
    """add vpns to redis"""
    with RedisConnection() as r:
        for vpn in vpns:
            r.zadd("vpns", {(vpn[0], vpn[1]): vpn[2]})
    return True
    
