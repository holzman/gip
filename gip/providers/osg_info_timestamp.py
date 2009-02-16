#!/usr/bin/python

import sys, time, os

# Make sure the gip_common libraries are in our path
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getTemplate, getLogger, printTemplate
from gip_cluster import getSubClusterIDs, getClusterID

# Retrieve our logger in case of failure
log = getLogger("GIP.timestamp")

def main():
    try:
        # Load up the site configuration
        cp = config()

        # Get the timestamp in the two formats we wanted
        epoch = str(time.time())
        now = time.strftime("%a %b %d %T UTC %Y", time.gmtime())

        # Load up the template for GlueLocationLocalID
        # To view its contents, see $VDT_LOCATION/gip/templates/GlueCluster
        template = getTemplate("GlueCluster", "GlueLocationLocalID")
        cluster_id = getClusterID(cp)
        for subClusterId in getSubClusterIDs(cp):
            # Dictionary of data to fill in for GlueLocationLocalID
            info = {'locationId':   'TIMESTAMP',
                    'subClusterId': subClusterId,
                    'clusterId':    cluster_id,
                    'locationName': 'TIMESTAMP',
                    'version':      epoch,
                    'path':         now,
                    }

            # Spit out our template, fill it with the appropriate info.
            printTemplate(template, info)

    except Exception, e:
        # Log error, then report it via stderr.
        log.error(e)
        sys.stdout = sys.stderr
        raise

if __name__ == '__main__':
    main()

