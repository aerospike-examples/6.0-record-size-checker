import aerospike
from aerospike_helpers import expressions as exp
from aerospike_helpers.operations import operations
from aerospike import exception as ex
import logging
import sys

# Define globals
namespace = "bar"
# If setName is an empty string then it will default to scanning all sets in the namespace
setName = ""
# threshold for compression ratio variance (Default: 10%)
threshold = 0.10
host = "34.173.191.40"
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

logging.basicConfig(format='%(asctime)s [%(levelname)s]: %(message)s', level=logLevel)


# Establishes a connection to the server
client = aerospike.client(config).connect()

# Check if cluster is stable
try:
    stable = client.info_all("cluster-stable:")
except ex.AerospikeError as e:
    logging.error("{0} [{1}]".format(e.msg, e.code))
    exit(1)

# Set global current node so we can keep track of returned object counts later
current_node = ""

def display_key(rec):
    k, _, _ = rec
    ns, setname, pk, digest = k[0], k[1], k[2], k[3].hex()
    compression_ratios[current_node]["count"] += 1
    try:
        # if dry_run is True it will only return the digests and object count at the end. Will NOT touch the records
        if not dry_run:
            client.touch(k,0)
        else:
            logging.info("Namespace: {0}, Set: {1}, Primary Key: {2}, Digest: {3}".format(ns, setname, pk, digest))
    except ex.AerospikeError as e:
        if e.code == 13:
            logging.warning("Record too big: Namespace: {0}, Set: {1}, Primary Key: {2}, Digest: {3}".format(ns, setname, pk, digest))
        elif e.code == 8:
            # if in stop writes no point in continuing to touch records on namespace
            logging.error("Unable to touch records due to no space in the namespace. Exiting.")
            exit(1)
        else:
            logging.error("{0} [{1}]".format(e.msg, e.code))
    except KeyboardInterrupt:
        logging.warning("Detected interrupt signal (CTRL+C) -- exiting.")
        sys.exit(1)

# Get write-block-size
all_conf = client.info_all("get-config:context=namespace;id={0}".format(namespace))
# Get compression ratios
namespace_stats = client.info_all("namespace/{0}".format(namespace))

compression_ratios = {}

# Obtain write-block-size for each node
for node, config in all_conf.items():
    params = config[1].split(';')
    compression_ratios[node] = {}
    for match in params:
        if "write-block-size" in match:
            compression_ratios[node]["wbs"] = int(match.split('=')[1])

# Obtain max-record-size and check if compression is enabled or not
for node, config in namespace_stats.items():
    params = config[1].split(';')
    compression_ratios[node]["count"] = 0   
    compression_ratios[node]["isCompressionEnabled"] = False
    for match in params:
        if "max-record-size" in match:
            mrs = int(match.split('=')[1])
            if mrs != (compression_ratios[node]["wbs"] - 16) and not dry_run:
                logging.warning("max-record-size is not set to (write-block-size - 16 bytes) for node {0} (write-block-size: {1}, max-record-size: {2})".format(node, compression_ratios[node]["wbs"], mrs))
                logging.warning("max-record-size needs to be configured to successfully identify potential large records.")
                exit(1)
            compression_ratios[node]["max-record-size"] = mrs
        elif "device_compression_ratio" in match:
            compression_ratios[node]["isCompressionEnabled"] = True
            compression_ratios[node]["compression_ratio"] = float(match.split('=')[1])

def areValuesInSync(list):
    return all(i == list[0] for i in list)

# Check if write-block-size is in sync across nodes
if areValuesInSync([wbs["wbs"] for wbs in compression_ratios.values()]) != True:
    logging.warning("write-block-size are not uniform across nodes!")
    logging.warning(compression_ratios)

# Check if max-record-size is set and in sync across nodes
if areValuesInSync([mrs["max-record-size"] for mrs in compression_ratios.values()]) != True:
    logging.warning("max-record-size are not uniform across nodes!")
else:
    logging.debug("Compression ratios per node: {0}".format(compression_ratios))

# Create scan object and retrieve only metadata of records
scan = client.scan(namespace, setName)
scan_opts = {
    'nobins': True
}


try:
    for node in list(compression_ratios.keys()):
        current_node = node
        logging.info("Scanning node: {0}".format(node))
        if compression_ratios[node]["isCompressionEnabled"] == True:
            logging.info("Node {0} has compression enabled with a ratio of {1} and write-block-size={2}".format(node, compression_ratios[node]["compression_ratio"], compression_ratios[node]["wbs"]))
            # Calculate rough threshold for compressed records that may exceed write-block-size
            bs = (compression_ratios[node]["wbs"] * compression_ratios[node]["compression_ratio"]) - (compression_ratios[node]["wbs"] * threshold)
            too_big_exp = exp.GE(exp.DeviceSize(), int(bs)).compile()
            scan_policy = {
                "expressions": too_big_exp
            }
            logging.info("Checking for records of compressed size larger than {0} bytes".format(int(bs)))
            scan.foreach(display_key, policy=scan_policy, options=scan_opts, nodename=node)
        else:
            logging.info("Node {0} does not have compression enabled.".format(node))
            bs = compression_ratios[node]["wbs"] - 16
            too_big_exp = exp.GE(exp.DeviceSize(), int(bs)).compile()
            scan_policy = {
                "expressions": too_big_exp
            }
            logging.info("Checking for records of compressed size larger than {0} bytes".format(int(bs)))
            scan.foreach(display_key, policy=scan_policy, options=scan_opts,  nodename=node)
except ex.InvalidNodeError:
    logging.error("Unable to scan node {0} because it's not active. Is it quiesced?".format(node))
except Exception as e:
    logging.error("Unable to perform scan on node {0}: {1}".format(node,e))



for node in compression_ratios.items():
    logging.info("Node: {0} Returned Record Count: {1}".format(node[0], node[1]["count"]))

# Close the connection to the server
client.close()