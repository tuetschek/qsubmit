from argparse import ArgumentParser
from qsubmit import Job
import sys
import os

def run_script(args):

    # adjust the arguments to the internal API
    args = vars(args)
    if args['interactive']:
        del args['command']
    if args['gpus'] == 0:
        del args['gpus']
        del args['gpu_mem']
    args['log_dir'] = args['logdir']
    args['dependencies'] = args['hold']
    del args['logdir']
    del args['interactive']
    del args['hold']


    if args['log_dir'] is not None:
        logdir = args['log_dir']
        try:
            os.makedirs(logdir,exist_ok=True)
        except PermissionError:
            print(f"Logdir {logdir} could not be created due to permission error. Exiting.", file=sys.stderr)
            sys.exit(1)

    if len(args['command']) == 1:
        args['command'] = args['command'][0]

    job = Job(**args)
    job.submit(print_cmd=sys.stderr)
    if job.jobid:
        print("Submitted job ID: %s" % job.jobid, file=sys.stderr)

def qsubmit_argparser(name="qsubmit",desc="Batch engine script submission wrapper"):
    ap = ArgumentParser(prog=name,description=desc)
    ap.add_argument('--location', type=str, help='Override location detection')
    ap.add_argument('--engine', type=str, help='Use the given batch engine (instead of location default)')
    ap.add_argument('-i', '--interactive', action='store_true', help='Run interactive shell instead of batch command')
    ap.add_argument('-n', '-name', '-jobname', '--name', '--jobname', help='Job name', default='qsubmit')
    ap.add_argument('-q', '--queue', help='Name of the queue to send the command to')
    ap.add_argument('-c', '-cpus', '--cpus', '--cores', help='Number of CPU cores to use', type=int, default=1)
    ap.add_argument('-g', '-gpus','--gpus', help='Number of GPUs to use', type=int, default=0)
    ap.add_argument('-M', '-gpu-mem', '--gpu-mem', help='Amount of GPU memory to use', default='1g')
    ap.add_argument('-m', '-mem', '--mem', help='Amount of memory to use', default='1g')
    ap.add_argument('-l', '-logdir', '--logdir', help='Directory where the log file will be stored')
    ap.add_argument('-w', '--hold', '--wait', help='Hold until jobs with the given IDs are completed',
                    nargs='*', default=[], type=int)
    ap.add_argument('command', nargs='*', help='The arguments for the command to be run')

    return ap

def main():
    ap = qsubmit_argparse()

    args=ap.parse_args()
    if not args.command and not args.interactive:
        ap.error('Cannot run with an empty batch job (use -i or input command).')
    run_script(args)

if __name__ == '__main__':
    main()

