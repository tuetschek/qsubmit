Qsubmit
=======

_Wrapper for batch engine submission commands (a Python remake of 
[Ondrej Bojar](https://ufal.mff.cuni.cz/ondrej-bojar)'s Perl qsubmit)._

This script attempts to abstract away from local batch engine quirks 
and create a unified job submission interface, wherever you run it.
It also allows you to send _commands_ instead of _scripts_ into the 
queue, and you can easily request an interactive shell using the same
set of options. 

It's a work-in-progress, now only supporting the local settings at
Charles University/UFAL (SLURM with local specific settings), with
some (not up-to-date) support for the Son of Grid Engine (SGE) system 
and [Grun](https://github.com/earonesty/grun) as well as local execution
(so you can run your script without a cluster engine and don't
have to change your workflow much).

Usage
-----

You can install the script using `pip`:
```
pip3 install git+https://github.com/ufal/qsubmit
```

Run the script to see the available options:
```
qsubmit -h
```

The general idea for executing a job is this:
```
qsubmit [modifiers] [-n job-name] [resources] command
```

Under `resources`, you can specify the required number of `--cpus`, `--gpus`, 
the target `--queue` name, the CPU `--mem`ory and `--gpu-mem`ory.

There are a few specific modifiers to `qsubmit`'s behavior:
* `--hold/--wait <jobid>` -- waits for a specific other job
* `--logdir` -- sets a target logfile directory (defaults to current directory)
* `--location/--engine` -- setting for the cluster engine (location defaults to `ufal`, 
    engine defaults to `slurm`). You can set the `--engine` to `console` to run locally.

In order to get an interactive shell instead of running a batch job, use
```
qsubmit --interactive [modifiers] [-n job-name] [resources]
```
Note that the command is empty in this case.


Contribution
------------

If you like the idea and would like to add your own local settings to
the mix, please do. Edit the [script file](bin/qsubmit) accordingly
and send me [a pull request](https://github.com/tuetschek/qsubmit/pulls)!

License
-------

Copyright (c) 2017-2022 Ondřej Dušek, Dominik Macháček

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
