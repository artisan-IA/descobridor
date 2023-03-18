import os
import sys
import time
import json
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

"""
{
    hour: sorted set{(vpn_code, last_accessed)}
}
"""

class GmapsWorker:
    def __init__(self, name):
        self.name = name
        self.connection, self.channel, self.queue_name = gmaps_scrape_queue()
        bind_client_to_gmaps_scrape(self.channel)

    def callback(self, ch, method, properties, body):
        self.ensure_vpn_freshness()
        gmaps_entry = json.loads(body)
        #extract_all_reviews(gmaps_entry)
        print(" [x] Received %r" % gmaps_entry)
        time.sleep(1)
        print(" [x] Done")
        
        
    def main(self):
        connection, channel, queue_name = gmaps_scrape_queue()
        bind_client_to_gmaps_scrape(channel)
        channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=False)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
        
    def ensure_vpn_freshness(self):
        """
        checks how old the curren vpn connection is
        if it's older than a specified time, 
        it will connect to a different one available in redis
        """
        print(' [*] Ensuring vpn freshness')
        with RedisConnection() as r:
            if r.connection.exists(f"{self.name}_current_vpn"):
                current_vpn_key = r.connection.get(f"{self.name}_current_vpn")
                current_vpn_started = r.connection.hget("vpns", current_vpn_key)
                if datetime.now().timestamp() - float(current_vpn_started) < VPN_WAIT_TIME_S:
                    print(" [v] Current vpn is still fresh")
                    return True
        
        
            
    # HELPERS
    
    @staticmethod
    def _make_vpn_key(vpn, hours):
        return f"{vpn}_{hours}"
    
    @staticmethod
    def _break_vpn_key(vpn_key: str):
        return vpn_key.split("_")
        
    def _decode_vpn_pair(self, vpn_key: bytes, vpn_value: bytes):
        v, last_used = vpn_key.decode('utf-8'), vpn_value.decode('utf-8')
        vpn, hours = self._break_vpn_key(v)
        return vpn, float(hours), float(last_used)

    def get_vpns_from_redis(self):
        with RedisConnection() as r:
            vpns = r.connection.hgetall("vpns")
        return pd.DataFrame(
            [self._decode_vpn_pair(k, v) for (k, v) in vpns.items() ],
            columns=['vpn', 'hours', 'last_used'])
        
    @staticmethod
    def _circle_gauss(x, mu, sigma):
        return norm(mu, sigma).pdf(x) + norm(mu+24, sigma).pdf(x)
        
    def get_best_vpn(self):
        vpns_df = self.get_vpns_from_redis()
        now = datetime.now().hour + datetime.now().minute / 60
        vpns_df = vpns_df[datetime.now().timestamp() - vpns_df['last_used'] > VPN_WAIT_TIME_S]
        if vpns_df.empty:
            return None, None
        
        vpns_df['prob'] = vpns_df.hours.apply(lambda x: self._circle_gauss(now, x, 1.5))
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
                        "--auth-user-pass", f"{os.environ['OPENVPN_CONFIGS_DIR']}/secrets"])
                with RedisConnection() as r:
                    r.connection.hset("vpns", best_vpn, datetime.now().timestamp())
                    r.connection.set(f"{self.name}_current_vpn", 
                                    self._make_vpn_key(best_vpn, time_slot))
            except Exception as e:
                print(f" [!] Error connecting to vpn {best_vpn}")
                print(e)
                continue
            else:
                print(' [*] Ensuring vpn freshness')
                time.sleep(1)
                print(' [v] So fresh!')
                
                return True
    
        time.sleep(VPN_NOTHING_WORKS_SLEEP_S)
        return False

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

