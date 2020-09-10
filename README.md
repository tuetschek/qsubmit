Qsubmit
=======

_Wrapper for batch engine submission commands (a Python remake of 
[Ondrej Bojar](https://ufal.mff.cuni.cz/ondrej-bojar)'s Perl qsubmit)._

This script attempts to abstract away from local batch engine quirks 
and create a unified job submission interface, wherever you run it.
It's a work-in-progress, now only supporting the local settings at
my current and former workplaces (Charles University/UFAL -- Son of Grid 
Engine, Heriot-Watt University/Robotarium -- SLURM).

Usage
-----

Requires Python 3. You can install the script using `pip`:
```
pip3 install git+https://github.com/tuetschek/qsubmit
```

Run the script to see the available options:
```
qsubmit -h
```


Contribution
------------

If you like the idea and would like to add your own local settings to
the mix, please do. Edit the [script file](bin/qsubmit) accordingly
and send me [a pull request](https://github.com/tuetschek/qsubmit/pulls)!

License
-------

Copyright (c) 2017-2020 Ondřej Dušek

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
