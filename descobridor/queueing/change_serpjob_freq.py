import subprocess
import argparse

"""
crontab example:
*/5 * * * * (cd /Users/eliza/artesania/descobridor 
    && PYTHONPATH=/Users/eliza/artesania/descobridor 
    .venv/bin/python descobridor/queueing/serp_sender.py 
    > /usr/local/var/log/serp_sender.log 2>>
    &1)
"""

def read_cronjobs():
    """Read the current cronjob list"""
    cronjob = subprocess.Popen(["crontab", "-l"], stdout=subprocess.PIPE)
    cronjobs = cronjob.stdout.read().decode("utf-8").split("\n")
    return cronjobs

def change_serp_job_time(cronjobs, new_time):
    for (i, line) in enumerate(cronjobs):
        if "serp_sender.py" in line:
            print(line)
            partitions = line.partition(" /")
            new_line = f"{new_time} /{partitions[2]}"
            cronjobs[i] = new_line
            
def postpone_job():
    cronjobs = read_cronjobs()
    change_serp_job_time(cronjobs, "0 16 8 * *")
    

def resume_job():
    cronjobs = read_cronjobs()
    change_serp_job_time(cronjobs, "*/10 * * * *")
            

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--postpone", action="store_true")
    args.add_argument("--resume", action="store_true")
    if args.postpone:
        postpone_job()
    elif args.resume:
        resume_job()
    else:
        raise ValueError("Please specify either --postpone or --resume")
