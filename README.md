# ctrader-fix-api

See example/client.py for v0.1 version

# Setup

* virtualenv -p python3 venv
* ./venv/bin/pip install -r requirements.txt

# Usage
``
usage: client.py [-h] [--version] -b BROKER -u USERNAME -p PASSWORD -s SERVER
                 [-v] [-t MAX_THREADS]

CTrader FIX async client.

optional arguments:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  -b BROKER, --broker BROKER
                        Broker name, usually first part of sender id.
  -u USERNAME, --username USERNAME
                        Account number.
  -p PASSWORD, --password PASSWORD
                        Account password.
  -s SERVER, --server SERVER
                        Host, ex hXX.p.ctrader.com.
  -v, --verbose         Increase verbosity level. -v to something somewhat
                        useful, -vv to full debug
  -t MAX_THREADS, --max-threads MAX_THREADS
                        Thread limit in thread pool. Default to symbol table
                        length.

``