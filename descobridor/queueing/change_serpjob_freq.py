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


def append_resume_job(cronjobs):
    """Append the job to the cronjob list"""
    cronjobs.append(
        "0 17 8 * * (cd /Users/eliza/artesania/descobridor "
        "&& PYTHONPATH=/Users/eliza/artesania/descobridor "
        ".venv/bin/python descobridor/queueing/change_serpjob_freq.py --resume"
        " > /usr/local/var/log/serp_sender.log 2>> "
        "&1)"
    )
    
    
def remove_resume_job(cronjobs):
    """Remove the job from the cronjob list"""
    for (i, line) in enumerate(cronjobs):
        if "change_serpjob_freq.py" in line:
            cronjobs.pop(i)
            break
    

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
    append_resume_job(cronjobs)
    # write the new cronjob list
    subprocess.Popen(["crontab", "-"], stdin=subprocess.PIPE).communicate(
        input="\n".join(cronjobs).encode("utf-8")
    )
    

def resume_job():
    cronjobs = read_cronjobs()
    change_serp_job_time(cronjobs, "*/10 * * * *")
    remove_resume_job(cronjobs)
    subprocess.Popen(["crontab", "-"], stdin=subprocess.PIPE).communicate(
        input="\n".join(cronjobs).encode("utf-8")
    )
            

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--postpone", action="store_true")
    args.add_argument("--resume", action="store_true")
    arguments = args.parse_args()
    if arguments.postpone:
        postpone_job()
    elif arguments.resume:
        resume_job()
    else:
        raise ValueError("Please specify either --postpone or --resume")
