#!/usr/bin/env python3

from qsubmit.qsubmit_script import *
from qsubmit import Job
from pathlib import Path
from copy import copy
from threading import Thread
import time

import sys
import os

class QruncmdJob:

    def __init__(self, i, workdir):
        self.index = i

        self.buffer = []

        self.fname = f"{workdir}/job_{i}"


    def insert(self, line):
        self.buffer.append(line)

    def submit(self):
        lock = self.fname+".lock"
        try:
            os.mkdir(lock)
        except FileExistsError:
            print("Something weird happens. There is already a locked job.")
            raise

        with open(self.fname,"w") as f:
            print("".join(self.buffer),end="",file=f)
        os.rmdir(lock)
        del self.buffer

    def is_completed(self):
        return os.path.exists(self.fname+".ok")

    def flush(self):
        with open(self.fname+".out","r") as f:
            sys.stdout.write(f.read())
        sys.stdout.flush()
        os.remove(self.fname)
        os.remove(self.fname+".out")
        os.remove(self.fname+".ok")

def temp_workdir_fname(pref):
    import string
    import random
    letters = string.ascii_lowercase
    h = ''.join(random.choice(letters) for i in range(8)) 
    return f"{pref}-{h}"

def main():

    # all the same arguments as qsubmit has...
    ap = qsubmit_argparser(name="qruncmd",desc="Runs the line processing command (one input line for one output line) on SIZE-sized sections of stdin in parallel qsubmit jobs, and prints the outputs to stdout. It goes through stdin only once and it uses constant working disk space.")

    # ...plus following ones
    ap.add_argument('--workdir', type=str, default=None, help="workdir, default is qruncmd-workdir-XXXXXXXXX where X stands for random letter")
    ap.add_argument('--jobs',"--workers", type=int, default=5, help="How many workers (qsubmit jobs) to start. The workers concurrently wait for jobs (stdin sections saved to workdir), claim them, process and return the outputs.")
    ap.add_argument('-s','--size', type=int, help="How many lines in one job section.", default=500)
    args = ap.parse_args()

    batch_size = args.size
    workdir = args.workdir
    if workdir is None:
        workdir = temp_workdir_fname("qruncmd-workdir")
    workers = args.jobs
    del args.workdir
    del args.size
    del args.jobs

    if not os.path.isdir(workdir):
        os.mkdir(workdir)
    else:
        print(f"Workdir {workdir} already exists, maybe it should be cleared first?",file=sys.stderr)

    ######### start workers



    def start_workers():
        started_workers = 0
        cmd = " ".join(args.command)
        for i in range(workers):
            # stdbuf avoids stucking data between the pipes
            wrapcmd = f"mkfifo {workdir}/out-fifo-worker-{i} && stdbuf -o0 python3 -m qsubmit.qwrapcmd {workdir} {workdir}/out-fifo-worker-{i} | stdbuf -o0 -i0 -e0 {cmd} > {workdir}/out-fifo-worker-{i} ; touch {workdir}/worker-{i}.end"

            v = copy(args)
            v.command = wrapcmd
            if v.name is None or v.name == "qsubmit":
                v.name = f"qruncmd-{i}"
            if v.logdir is None:
                v.logdir = workdir

            # run qsubmit Job
            run_script(v)
            started_workers += 1
            if started_workers % 10 == 0:
                time.sleep(1)
        print("all the workers have started",file=sys.stderr)

    if workers > 10:
        starting_thread = Thread(target=start_workers)
        starting_thread.start()
    else:
        start_workers()
        starting_thread = None


    ######### 

    def slowpoison():
        # this will make the workers to complete the pending jobs and stop
        d = f"{workdir}/slow-poison-pill"
        Path(d).touch()

    max_jobs = 2*workers

    current_jobs = []

    global iseof
    iseof = False  # True when the input is over

    ######## processing loop

    global stop_everything

    stop_everything = False

    def submitting_loop():
        jobid = 0
        global iseof
        while not stop_everything:
            if not iseof and len(current_jobs) < max_jobs:
                j = QruncmdJob(jobid, workdir)
                jobid += 1
                for i in range(batch_size):
                    line = sys.stdin.readline()
                    if line != "":
                        j.insert(line)
                    else:
                        iseof = True
                        slowpoison()
                        break
                current_jobs.append(j)
                j.submit()
            else:
                print(f"submitting loop is idle, {len(current_jobs)} < {max_jobs}",file=sys.stderr)
                time.sleep(1)
        print("submitting completed",file=sys.stderr)

    def flushing_loop():
        global stop_everything
        while not stop_everything:
            while current_jobs and current_jobs[0].is_completed():
                j = current_jobs.pop(0)
                j.flush()
                print(f"flushing job {j.index}",file=sys.stderr)

            if iseof and not current_jobs:
                stop_everything = True
                break
            time.sleep(1)
        print("flushing completed",file=sys.stderr)

            # TODO: check the error status of the jobs, make sure there is no error...

    submit_thread = Thread(target=submitting_loop)
    submit_thread.start()

    flushing_loop()

    submit_thread.join()
    if starting_thread is not None:
        starting_thread.join()

if __name__ == "__main__":
    main()
