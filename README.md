# asdb-6.0-storage-change-record-checker
Simple python script to help detect records that potentially may be too big when upgrading from servers pre-6.0 to servers 6.0+.

## Tunable Variables
```python
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

# Client config and policies
config = {
    'hosts': [ (host, port) ],
    'user': 'admin',
    'password': 'admin123',
    # Default to INTERNAL auth but can be changed to AUTH_EXTERNAL or AUTH_EXTERNAL_INSECURE if needed (e.g. LDAP) 
    'policies': {
        'auth_mode': aerospike.AUTH_INTERNAL
    },
    # Change the below to True if needing to use alternate-access-address
    'use_services_alternate': True
}
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

## Example Output
When running with `dry_run = True` the script will print out the record metadata including the digest. 
```bash
❯ python3 main.py
2023-08-02 17:44:53,097 [INFO]: Scanning node: BB9C00F800A0142
2023-08-02 17:44:53,098 [INFO]: Node BB9C00F800A0142 does not have compression enabled.
2023-08-02 17:44:53,098 [INFO]: Checking for records of compressed size larger than 1048560 bytes
2023-08-02 17:46:59,366 [INFO]: Namespace: bar, Set: testset, Primary Key: None, Digest: fe0f17700e1b7fcc82401b535f7933667634f8bf
2023-08-02 17:46:59,366 [INFO]: Namespace: bar, Set: testset, Primary Key: None, Digest: fe0f14b652ecbaf4afc46de605d7a4a0b6452f3a
...
2023-08-02 17:46:59,366 [INFO]: Node: BB9C00F800A0142 Returned Record Count: 328633
2023-08-02 17:46:59,366 [INFO]: Node: BB93E00800A0142 Returned Record Count: 330107
2023-08-02 17:46:59,366 [INFO]: Node: BB93F00800A0142 Returned Record Count: 329541
```

You can either set `dry_run = False` to have the script touch each digest as its read from the scan results, or you can create a list of the digests and create your own client to perform the touch() calls.
Upon touching the digest, the client will be return an error of "Record too big" which the record(s) would unable to be migrated to 6.0+ servers.
