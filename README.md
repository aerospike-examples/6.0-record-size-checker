# asdb-6.0-storage-change-record-checker
Simple python script to help detect records that potentially may be too big when upgrading from servers pre-6.0 to servers 6.0+.

## Tunable Variables
```bash
# Define globals
namespace = "test"
# If setName is an empty string then it will default to scanning all sets in the namespace
setName = ""
# threshold for compression ratio variance (Default: 10%)
threshold = 0.10
host = "127.0.0.1"
port = 3000
# log level - Default INFO
# change to logging.DEBUG for more verbose logging
logLevel = logging.INFO # logging.DEBUG
# If dry_run is True then the script will only return the count of master objects on each node
# that are greater than or equal to the DeviceSize() filter applied
dry_run = True
```

## Running
Ensure the [aerospike-client-python](https://github.com/aerospike/aerospike-client-python) library is installed:
```bash
pip3 install aerospike
```

Execute the python script:
```bash
python3 main.py
```
