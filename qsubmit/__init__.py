#!/usr/bin/env python
# coding=utf-8

import os
import subprocess
from tempfile import NamedTemporaryFile
import string
import random
import re
import time
import collections
import socket
import shlex


"""Interface for running any Python code as a job on the cluster
(using the qsub/qstat/qacct commands).

Tested with Sun Grid Engine.
"""

# TODO: allow .qsubmitrc to override this
LOCATIONS = {
    'bwlf': {
        'engine': 'grun',
        'hostname': r'(bwlf.*|jove|amaterasu|osiris)',
    },
    'robotarium': {
        'engine': 'slurm',
        'hostname': r'.*cm\.cluster',
    },
    'ufal': {
        'engine': 'sge',
        'hostname': r'.*ms\.mff\.cuni\.cz',
    },
}


ENGINES = {
    'sge': {
        'submit_cmd': 'qsub -cwd -j y -o "<LOGDIR><NAME>.o$JOB_ID"',
        'interactive_cmd': 'qrsh -now no -pty yes',
        'delete_cmd': 'qdel <JOB_ID>',
        'params': {
            'name': '-N "<NAME>"',
            'mem': '-l mem_free=<MEM>,act_mem_free=<MEM>,h_vmem=<MEM>',
            'cpus': '-pe smp <CPUS>',
            'queue': '-q <QUEUE>',
            'hold': '-hold_jid <HOLD>',
            'gpus': '-l gpu=<GPUS>,gpu_ram=<GPU_MEM>',
        },
        'script': {
            'print_info': 'qstat -j <JOB_ID>',
            'resource_cmd': 'qstat -j $JOB_ID | grep -e "^hard resource_list" | cut -d " " -f 11-; echo NSLOTS=$NSLOTS',
            'info_cmd': 'echo "NOT IMPLEMENTED"',
            'maxvmem_cmd': 'qstat -j $JOB_ID | grep -e "^usage" | cut -f 5 -d, | cut -d = -f 2',
            'usage_cmd': 'qstat -j $JOB_ID | grep "^usage" | cut -b 29-',
            'load_profile_cmd': '[ -e /net/projects/SGE/user/sge_profile ] && . /net/projects/SGE/user/sge_profile',
        }
    },
    'grun': {
        'submit_cmd': 'grun -oe "<LOGDIR><NAME>.o%j" -nowait',
        'script_prepend': '/bin/bash',
        'params': {
            'name': '-j "<NAME>"',
            'mem': '--mem=<MEM>',
            'cpus': '-c <CPUS>',
            'hold': '-hold_jid <HOLD>',
        },
        'script': {
            'print_info': 'echo "NOT IMPLEMENTED"',
            'resource_cmd': 'echo "NOT IMPLEMENTED"',
            'info_cmd': 'echo "NOT IMPLEMENTED"',
            'maxvmem_cmd': 'echo "NOT IMPLEMENTED"',
            'usage_cmd': 'echo "NOT IMPLEMENTED"',
            'load_profile_cmd': '',
        }
    },
    'slurm': {
        'submit_cmd': 'sbatch -o <LOGDIR><NAME>.o%j',
        'params': {
            'name': '-J <NAME>',
            'mem': '--mem=<MEM>',
            'cpus': '-c <CPUS>',
            'queue': '-p <QUEUE>',
            'hold': '-d afterany:<HOLD>',
        },
        'script': {
            'print_info': 'echo "NOT IMPLEMENTED"',
            'resource_cmd': 'echo "NOT IMPLEMENTED"',
            'info_cmd': 'echo "NOT IMPLEMENTED"',
            'maxvmem_cmd': 'echo "NOT IMPLEMENTED"',
            'usage_cmd': 'echo "NOT IMPLEMENTED"',
            'load_profile_cmd': '',
        }
    },
    'console': {
        'submit_cmd': 'bash',
        'params': {},
        'script': {
            'print_info': 'echo "NOT IMPLEMENTED"',
            'resource_cmd': 'echo "NOT IMPLEMENTED"',
            'info_cmd': 'echo "NOT IMPLEMENTED"',
            'maxvmem_cmd': 'echo "NOT IMPLEMENTED"',
            'usage_cmd': 'echo "NOT IMPLEMENTED"',
            'load_profile_cmd': '',
        }
    }
}


DEFAULT_SCRIPT_TEMPLATE = '''#!/bin/bash

sdate=`date`  # start date

# load UFAL SGE profile, if exists
<LOAD_PROFILE_CMD>

hard=$(<RESOURCE_CMD>)

echo "=============================="
echo "== Server:    "`hostname`
echo "== Directory: "`pwd`
echo '== Command:   <MAIN_CMD_ESC>'
echo "== Hard res:  $hard"
echo "== Started:   $sdate"
echo "== Sourcing:  $HOME/.bashrc"
echo "=============================="

# Source the bashrc
. $HOME/.bashrc

# Renice ourselves
renice 10 $$

echo "=============================="

# Run the command and collect exit status
<MAIN_CMD>
exitstatus=$?

if [ 0 != "$exitstatus" ]; then
  exitinfo="FAILED (exit status $exitstatus)"
fi

echo "=============================="

fdate=`date`

# remove this temporary script
rm <SCRIPT_TMPFILE>

# print all we know about ourselves
echo "Batch job information:"
<INFO_CMD>

echo "Getting usage and peak mem info (works for SGE, not PBS yet)..."
usage=$(<USAGE_CMD>)
maxvmem=$(<MAXVMEM_CMD>)

duration=$SECONDS
duration=$((duration / 3600)):`printf '%02d' $(((duration / 60) % 60))`:`printf '%02d' $((duration % 60))`

echo "=============================="
echo "== Server:    "`hostname`
echo "== Directory: "`pwd`
echo '== Command:   <MAIN_CMD_ESC>'
echo "== Usage:     $usage"
echo "== Peak mem:  $maxvmem"
echo "== Started:   $sdate"
echo "== Finished:  $fdate     $exitinfo"
echo "== Duration:  $duration"
echo "=============================="
'''

# default job header
DEFAULT_CODE_TEMPLATE = """#!/usr/bin/env python3
import os

def main():
<CODE>

if __name__ == '__main__':
    main()
    os.remove('<CODE_TMPFILE>')
"""


def detect_location():
    """Check for hostname patterns, if they correspond to any of the preset locations."""
    hostname = socket.getfqdn()
    for loc, params in LOCATIONS.items():
        if re.search(params['hostname'], hostname):
            return loc
    raise Exception('Qsubmit not configured to use at %s' % hostname)


class Job:
    """This represents a piece of code as a job on the cluster, holds
    information about the job and is able to retrieve job metadata.

    The most important method is submit(), which submits the given
    piece of code to the cluster.

    Important attributes (some may be set in the constructor or
    at job submission, but all may be set between construction and
    launch):
    ------------------------------------------------------------------
    name      -- job name on the cluster (and the name of the created
                 Python script, default will be generated if not set)
    code      -- the Python code to be run (needs to have imports and
                 sys.path set properly)
    header    -- the header of the created Python script (may contain
                 imports etc.)
    mem       -- the amount of memory to reserve for this job on the
                 cluster
    cpus     -- the number of cpus needed for this job
    work_dir  -- the working directory where the job script will be
                 created and run (will be created on launch)
    dependencies-list of Jobs this job depends on (must be submitted
                 before submitting this job)
    queue     -- queue setting for SGE

    In addition, the following values may be queried for each job
    at runtime or later:
    ------------------------------------------------------------------
    submitted -- True if the job has been submitted to the cluster.
    state     -- current job state ('qw' = queued, 'r' = running, 'f'
                 = finished, only if the job was submitted)
    host      -- the machine where the job is running (short name)
    jobid     -- the numeric id of the job in the cluster (NB: type is
                 string!)
    report    -- job report using the qacct command (dictionary,
                 available only after the job has finished)
    exit_status- numeric job exit status (if the job is finished)
    """

    # job state 'FINISHED' symbol
    FINISH = 'f'
    # job name prefix
    NAME_PREFIX = 'qsubmit_'
    # job directory prefix
    DIR_PREFIX = '_qsubmit-'
    # legal chars for generated job names
    JOBNAME_LEGAL_CHARS = string.ascii_letters + string.digits
    # default number of cpus
    DEFAULT_CPUS = 1
    # default memory size
    DEFAULT_MEMORY = '4g'
    DEFAULT_GPU_MEM = '4g'
    # only 1 job status query per second
    TIME_QUERY_DELAY = 1
    # job status polling delay for wait() in seconds
    TIME_POLL_DELAY = 60

    def __init__(self, code=None, command=None,
                 name=None, work_dir=None, dependencies=None,
                 mem=DEFAULT_MEMORY, cpus=DEFAULT_CPUS,
                 gpus=None, gpu_mem=DEFAULT_GPU_MEM,
                 engine=None, location=None, queue=None,
                 code_templ=DEFAULT_CODE_TEMPLATE, script_templ=DEFAULT_SCRIPT_TEMPLATE):
        """Constructor. May provide some running options --
        the desired Python code to be run, the headers of the resulting
        script (default provided), the job name and working directory.
        All of these options can be set later via the corresponding
        attributes.
        """
        if engine:
            self.engine = ENGINES[engine]
        else:
            location = location or detect_location()
            self.engine = ENGINES[LOCATIONS[location]['engine']]
        self.code = code
        self.command = command
        self.code_templ = code_templ
        self.script_templ = script_templ
        self.mem = mem
        self.cpus = cpus
        self.gpus = gpus
        self.gpu_mem = gpu_mem
        self.queue = queue
        self._jobid = None
        self._host = None
        self._state = None
        self._report = None
        self._state_last_query = time.time()
        self._dependencies = []
        if dependencies is not None:
            self.add_dependency(dependencies)
        self._name = name if name is not None else self._generate_name()
        self.submitted = False
        self.work_dir = work_dir if work_dir is not None else os.getcwd()

    def submit(self, print_cmd=None):
        """Submit the job to the cluster.
        All jobs on which this job is dependent must already be submitted!
        """
        # create working directory if necessary
        if not os.path.isdir(self.work_dir):
            os.mkdir(self.work_dir)
        cwd = os.getcwd()
        os.chdir(self.work_dir)

        run_cmd = self.engine['submit_cmd']
        # python code
        if self.code:
            script_file = self._get_code_script()
        # any command
        elif self.command:
            script_file = self._get_command_script()
        # interactive
        else:
            run_cmd = self.engine['interactive_cmd']

        # get the engine params (replace job name + logdir in the main command)
        run_cmd = run_cmd.replace('<NAME>', self.name or 'qsubmit')
        run_cmd = run_cmd.replace('<LOGDIR>', (self.work_dir or '.') + '/')
        run_cmd = shlex.split(run_cmd)

        # add resource requests and dependencies
        run_cmd.extend(self._get_resource_requests())
        run_cmd.extend(self._get_dependency_string())

        # add the scriptfile name, or login shell
        if self.code or self.command:
            if 'script_prepend' in self.engine:
                run_cmd.append(self.engine['script_prepend'])
            run_cmd.append(script_file.name)
        else:
            run_cmd.extend(['bash', '-l'])

        # submit the script
        self.submit_cmd = ' '.join([shlex.quote(t) for t in run_cmd])
        if print_cmd is not None:
            print(self.submit_cmd, file=print_cmd)
        if self.code or self.command:
            output = subprocess.check_output(run_cmd, encoding='UTF-8')
            self._jobid = re.search('([0-9]+)', output).group(0)
        else:
            subprocess.call(run_cmd)

        os.chdir(cwd)
        self.submitted = True

    @property
    def state(self):
        """Retrieve information about current job state. Will also
        retrieve the host this job is running on and store it in
        the __host variable, if applicable.
        """
        # job hasn't been submitted -- no point in retrieving state
        if not self.submitted:
            return None
        # state caching
        if time.time() < self._state_last_query + self.TIME_QUERY_DELAY:
            return self._state
        self._state_last_query = time.time()
        # actually retrieve the state
        state, host = self._get_job_state()
        self._state = state
        if state != self.FINISH:
            self._host = host
        return self._state

    @property
    def report(self):
        """Access to qacct report. Please note that running the qacct command
        takes a few seconds, so the first access to the report is rather
        slow.
        """
        # no stats until the job has finished
        if not self.submitted or self.state != self.FINISH:
            return None
        # the report is retrieved only once
        if self._report is None:
            # try to retrieve the qacct report
            output = subprocess.check_output('qacct -j ' + self.jobid, encoding='UTF-8')
            self._report = {}
            for line in output.split("\n")[1:]:
                key, val = re.split(r'\s+', line, 1)
                self._report[key] = val.strip()
        return self._report

    @property
    def exit_status(self):
        """Retrieve the exit status of the job via the qacct report.
        Throws an exception the job is still running and the exit status
        is not known.
        """
        report = self.report
        if report is None:
            raise RuntimeError('Job {self.jobid} is probably still running')
        return int(report['exit_status'])

    def wait(self, poll_delay=None):
        """Waits for the job to finish. Will raise an exception if the
        job did not finish successfully. The poll_delay variable controls
        how often the job state is checked.
        """
        poll_delay = poll_delay if poll_delay else self.TIME_POLL_DELAY
        while self.state != self.FINISH:
            time.sleep(poll_delay)
        if self.exit_status != 0:
            raise RuntimeError(f'Job {self.name} ({self.jobid}) did not finish successfully.')

    def add_dependency(self, dependency):
        """Adds a dependency on the given Job(s).
        """
        if isinstance(dependency, Job) or isinstance(dependency, str):
            self._dependencies.append(dependency)
        elif isinstance(dependency, int):
            self._dependencies.append(str(dependency))
        elif isinstance(dependency, collections.Iterable):
            for dep_elem in dependency:
                self.add_dependency(dep_elem)
        else:
            raise ValueError('Unknown dependency type!')

    def remove_dependency(self, dependency):
        """Removes the given Job(s) from the dependencies list.
        """
        # single element removed
        if isinstance(dependency, (Job, str, int)):
            if isinstance(dependency, int):
                jobid = str(dependency)
            else:
                jobid = dependency
            rem = next((d for d in self._dependencies if d == jobid), None)
            if rem is not None:
                self._dependencies.remove(rem)
            else:
                raise ValueError('Cannot find dependency!')
        elif isinstance(dependency, collections.Iterable):
            for dep_elem in dependency:
                self.remove_dependency(dep_elem)
        else:
            raise ValueError('Unknown dependency type!')

    def delete(self):
        """Delete this job."""
        if self.submitted:
            subprocess.check_output('qdel ' + self.jobid, encoding='UTF-8')

    @property
    def host(self):
        """Retrieve information about the host this job is/was
        running on.
        """
        # no point if the job has not been submitted
        if not self.submitted:
            return None
        # return a cached value
        if self._host is not None:
            return self._host
        # try to get state and return the stored value
        self.state()
        return self._host

    @property
    def name(self):
        """Return the job name.
        """
        return self._name

    @property
    def jobid(self):
        """Return the job id.
        """
        return self._jobid

    def _get_code_script(self):
        """Join headers and code to create a meaningful Python script."""
        # create a script tempfile
        script_tmpfile = NamedTemporaryFile(mode='w', suffix='.py', prefix='.qsubmit-', dir=os.getcwd(), encoding='UTF-8', delete=False)

        script_text = self.code_templ
        script_text = script_text.replace('<CODE>', re.sub('^', '    ', self.code, 0, re.MULTILINE))
        script_text = script_text.replace('<CODE_TMPFILE>', script_tmpfile.name)

        script_tmpfile.write(script_text)
        script_tmpfile.close()
        return script_tmpfile

    def _get_command_script(self):
        # create a script tempfile
        script_tmpfile = NamedTemporaryFile(mode='w', suffix='.bash', prefix='.qsubmit-', dir=os.getcwd(), encoding='UTF-8', delete=False)

        # create script text
        script_text = self.script_templ
        for var_name, value in self.engine['script'].items():
            script_text = script_text.replace('<' + var_name.upper() + '>', value)
        script_text = script_text.replace('<SCRIPT_TMPFILE>', script_tmpfile.name)
        main_cmd = " ".join(self.command)
        script_text = script_text.replace('<MAIN_CMD>', main_cmd)
        script_text = script_text.replace('<MAIN_CMD_ESC>', main_cmd.replace("'", "'\"'\"'"))

        # print it into a tempfile and close it
        script_tmpfile.write(script_text)
        script_tmpfile.close()
        return script_tmpfile

    def _generate_name(self):
        """Generate a job name"""
        return self.NAME_PREFIX + ''.join([random.choice(self.JOBNAME_LEGAL_CHARS) for _ in range(5)])

    def _get_work_dir(self):
        """Generate a valid working directory name"""
        num = 1
        workdir = None
        while workdir is None or os.path.exists(workdir):
            workdir = os.path.join(os.getcwd(), self.DIR_PREFIX + self.name + '-' + str(num).zfill(3))
            num += 1
        return workdir

    def _get_resource_requests(self):
        """Generate qsub resource requests based on the mem and core setting."""
        res = []
        for param, param_str in self.engine['params'].items():
            val = getattr(self, param, None)
            if val is None:
                continue
            param_str = param_str.replace('<' + param.upper() + '>', str(val))
            # replace any secondary parameters tied to this one
            # e.g. GPU memory for # GPUs
            for m in reversed(list(re.finditer('<([A-Z_]+)>', param_str))):
                secondary_param = m.group(1).lower()
                secondary_val = getattr(self, secondary_param, None)
                if secondary_val:
                    param_str = param_str[:m.start()] + secondary_val + param_str[m.end():]
            res.extend(shlex.split(param_str))
        return res

    def _get_dependency_string(self):
        """Generate qsub dependency string based on the list of dependencies."""
        if self._dependencies:
            if not all([dep.submitted if isinstance(dep, Job) else True
                        for dep in self._dependencies]):
                raise RuntimeError('Job has unsubmitted dependencies!')
            hold_str = ','.join([dep.jobid if isinstance(dep, Job) else dep for dep in self._dependencies])
            return shlex.split(self.engine['params']['hold'].replace('<HOLD>', hold_str))
        return []

    def _get_job_state(self):
        """Parse the qstat command and try to retrieve the current job
        state and the machine it is running on."""
        # get state of job assuming it is in the queue
        output = subprocess.check_output('qstat', encoding='UTF-8')
        # get the relevant line of the qstat output
        output = next((l for l in output.split("\n") if re.search(self.jobid, l)), None)
        # job does not exist anymore
        if output is None:
            return self.FINISH, None
        # parse the correct line:
        fields = re.split(r'\s+', output.strip())
        state, host = fields[4], fields.get(7)
        host = re.sub(r'.*@([^.]+)\..*', r'\1', host) if '@' in host else ''
        return state, host

    def __eq__(self, other):
        """Comparison: based on ids or reference if ids are None."""
        if self._jobid is not None and other.__jobid is not None:
            return self._jobid == other.__jobid
        return self == other

    def __str__(self):
        """String representation returns the attribute name and type."""
        return f'{self.__class__.__name__}: {self.name} ({self.work_dir})'
