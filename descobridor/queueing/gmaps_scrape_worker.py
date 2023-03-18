import os
import sys
import time
import json
from typing import Tuple
import pandas as pd
from scipy.stats import norm
from datetime import datetime
import subprocess

from truby.db_connection import RedisConnection

from descobridor.queueing.queues import gmaps_scrape_queue, bind_client_to_gmaps_scrape
from descobridor.discovery.read_raw_reviews import extract_all_reviews # noqa F401
from descobridor.queueing.constants import (
    VPN_WAIT_TIME_S, VPN_NOTHING_WORKS_SLEEP_S
)


class GmapsWorker:
    def __init__(self, name: str):
        self.name = name
        self.connection, self.channel, self.queue_name = gmaps_scrape_queue()
        bind_client_to_gmaps_scrape(self.channel)
        
    @property
    def current_vpn_key(self):
        return f"{self.name}_current_vpn"

    def callback(self, ch, method, properties, body: bytes) -> None:
        """
        checks that all is good with the vpn
        then scrapes google maps
        """
        self.ensure_vpn_freshness()
        gmaps_entry = json.loads(body)
        # extract_all_reviews(gmaps_entry)
        print(" [x] Received %r" % gmaps_entry)
        time.sleep(10)
        print(" [x] Done")  
        
    def main(self) -> None:
        connection, channel, queue_name = gmaps_scrape_queue()
        bind_client_to_gmaps_scrape(channel)
        channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=False)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
        
    # callback actions
        
    def ensure_vpn_freshness(self) -> bool:
        """
        checks how old the curren vpn connection is
        if it's older than a specified time, 
        it will connect to a different one available in redis
        """
        print(' [*] Ensuring vpn freshness')
        with RedisConnection() as r:
            # if there's a current vpn assigned to this worker
            if r.connection.exists(self.current_vpn_key):
                if self.is_connected():
                    return True
        
        print(" [*] Changinging the VPN")
        # TODO how to disconnect from previous one??? Do we need to?
        # https://askubuntu.com/questions/298419/how-to-disconnect-from-openvpn
        # seems could be fun
        is_connected = self.connect_to_a_new_vpn()
        if not is_connected:
            print(" [!] No vpn available, waiting a bit")
            time.sleep(VPN_NOTHING_WORKS_SLEEP_S)
            raise NoVPNError("No vpn available")
        return True
        
    def is_connected(self):
        # TODO check if connected to any vpn! ifconfig -> could look different on Ubuntu
        return True
    
    def get_best_vpn(self):
        """
        vpns pretend to be people with typical schedules.
        Schedules are defined by preffered time slots and stored in redis hset together 
        with times when the vpn was last used
        {"<vpn_config_file>_<preffered_time>: <last_used timestamp>}
        We want to connect to the vpn that haven't been used in a while
        and that is which preffered time slot is closest to the current time.
        """
        vpns_df = self._get_vpns_from_redis()
        now = datetime.now().hour + datetime.now().minute / 60
        vpns_df = vpns_df[datetime.now().timestamp() - vpns_df['last_used'] > VPN_WAIT_TIME_S]
        if vpns_df.empty:
            return None, None
        
        vpns_df['prob'] = vpns_df.hours.apply(lambda x: self._affinity(now, x, 1.5))
        best_vpn = vpns_df.sort_values('prob', ascending=False).iloc[0]
        return best_vpn.vpn, best_vpn.hours
        
    def connect_to_a_new_vpn(self):
        for _ in range(5):         
            best_vpn, time_slot = self.get_best_vpn()
            if best_vpn is None:
                break
            try:
                subprocess.run(
                    ["openvpn", "--config", f"{os.environ['OPENVPN_CONFIGS_DIR']}/{best_vpn}",
                        "--auth-user-pass", f"{os.environ['OPENVPN_CONFIGS_DIR']}/secrets", "&"])
                self._update_redis_records_new_vpn(best_vpn, time_slot)
                
            except Exception as e:
                print(f" [!] Error connecting to vpn {best_vpn}")
                print(e)
                continue
            else:
                print(' [v] So fresh!')
                
                return True
    
        return False
        
        
            
    # HELPERS
    
    @staticmethod
    def _make_vpn_key(vpn: str, time_slot: float) -> str:
        """
        :param vpn: name of the vpn config file
        :param time_slot: time slot in which the vpn is preffered eg 23.6
        """
        return f"{vpn}_{time_slot}"
    
    @staticmethod
    def _break_vpn_key(vpn_key: str) -> Tuple[str, float]:
        print(vpn_key)
        return vpn_key.split("_")
        
    def _decode_vpn_pair(self, vpn_key: bytes, vpn_value: bytes):
        v, last_used = vpn_key.decode('utf-8'), vpn_value.decode('utf-8')
        vpn, hours = self._break_vpn_key(v)
        return vpn, float(hours), float(last_used)

    def _get_vpns_from_redis(self) -> pd.DataFrame:
        with RedisConnection() as r:
            vpns = r.connection.hgetall("vpns")
            
        return pd.DataFrame(
            [self._decode_vpn_pair(k, v) for (k, v) in vpns.items()],
            columns=['vpn', 'hours', 'last_used'])
        
    @staticmethod
    def _affinity(x: float, mu: float, sigma: float) -> float:
        """
        could be just distance really
        :return: measure of distance between x and mu
        """
        return norm(mu, sigma).pdf(x) + norm(mu+24, sigma).pdf(x)
    
    def _update_redis_records_new_vpn(self, best_vpn: str, time_slot: float) -> None:
        with RedisConnection() as r:
            r.connection.hset("vpns", 
                                self._make_vpn_key(best_vpn, time_slot), 
                                datetime.now().timestamp())
            r.connection.set(self.current_vpn_key, 
                            self._make_vpn_key(best_vpn, time_slot), 
                            ex=VPN_WAIT_TIME_S)
            

if __name__ == '__main__':
    try:
        gmaps_worker = GmapsWorker("macbook")
        gmaps_worker.main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)


class NoVPNError(Exception):
    pass
