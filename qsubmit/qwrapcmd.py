#!/usr/bin/env python3

# This is a tool that is supposed to be used only internally by qruncmd. It doesn't have a nice entry point on purpose.

# Usage:
#
# python3 -m qsubmit.qwrapcmd {workdir} {workdir}/out-fifo-worker-{i} | stdbuf -oL {cmd} > {workdir}/out-fifo-worker-{i}

# - cmd is a command (worker) that receives stdin and produces one line of output for each input line
# - qwrapcmd.py is a worker wrapper: 
#   - waits for a job, acquires and locks it
#   - sends the job input from job_{j} file to cmd behind a pipe on stdout
#   - collects output of cmd from the named pipe out-fifo-worker-{i}
#   - it saves the output to job_{j}.out file, markes the job as OK by touching job_{j}.ok file, and releases the lock
#   - dies on a poison pill

import sys
import os
from pathlib import Path
import time
import threading

workdir = sys.argv[1]
fifofn = sys.argv[2]
job_pref = "job"

out = open(fifofn,"r")

#global lines

#lines = 0
def process_job(inname):
    global complete
    complete = False
    lines = 0
    def reading():
        global complete
        received = 0
        with open(inname+".out","w") as outf:
            while True:
                if received < lines:
                    r = out.readline()
                    print(r,end="",file=outf)
                    received += 1
                elif received == lines:
                    if complete:
                        break
                else:
                    time.sleep(0.1)
        print("done",file=sys.stderr)
    t = threading.Thread(target=reading)
    t.start()

    print(f"Processing {inname}",file=sys.stderr)
    with open(inname,"r") as f:
        for line in f:
            print(line,end="",flush=True)
            lines += 1
            if lines % 100 == 0:
                print("line",lines,file=sys.stderr)
    complete = True
    print(f"waiting for {lines} lines",file=sys.stderr)
    t.join()


def sortjobs(jobs):
    x = [ int(j.replace(job_pref+"_","")) for j in jobs ]
    x = sorted(x)
    return [ job_pref+"_" + str(i) for i in x ]


while not os.path.exists(f"{workdir}/fast-poison-pill"):  # to be implemented in qruncmd.py
    iswork = False

    jobs = []
    for fn in os.listdir(workdir):
        if fn.startswith(job_pref) and not fn.endswith("lock") and not fn.endswith("ok") and not fn.endswith("out"):
            jobs.append(fn)
    for fn in sortjobs(jobs):
        dfn = f"{workdir}/{fn}"
        lock = f"{dfn}.lock"
        ok = f"{dfn}.ok"
        if not os.path.exists(ok) and not os.path.isdir(lock):
            try:
                os.mkdir(lock)
            except FileExistsError:
                continue
            process_job(dfn)
            Path(ok).touch()
            os.rmdir(lock)
            iswork = True
    if not iswork and os.path.exists(f"{workdir}/slow-poison-pill"):
        break
    time.sleep(0.01)
out.close()
