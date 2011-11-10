"""
Common functions for GIP batch system providers and plugins.
"""

from gip_common import cp_getBoolean, cp_get, cp_getList, cp_getInt
from gip_cluster import getOSGVersion
from gip_sections import ce

__author__ = "Burt Holzman"

def buildCEUniqueID(cp, ce_name, batch, queue):
    ce_prefix = 'jobmanager'
    if cp_getBoolean(cp, 'cream', 'enabled', False):
        ce_prefix = 'cream'

    port = getPort(cp)
    ce_unique_id = '%s:%d/%s-%s-%s' % (ce_name, port, ce_prefix, batch, queue)
    return ce_unique_id

def getGramVersion(cp):
    gramVersion = '\n' + 'GlueCEInfoGRAMVersion: 5.0'
    if cp_getBoolean(cp, 'cream', 'enabled', False):    
        gramVersion = ''

    return gramVersion
        
def getCEImpl(cp):
    ceImpl = 'Globus'
    ceImplVersion = cp_get(cp, ce, 'globus_version', '4.0.6')    
    if cp_getBoolean(cp, 'cream', 'enabled', False):
        ceImpl = 'CREAM'
        ceImplVersion = getOSGVersion(cp)
    return (ceImpl, ceImplVersion)

def getPort(cp):
    port = 2119
    if cp_getBoolean(cp, 'cream', 'enabled', False):
        port = 8443
    return port
    
def buildContactString(cp, batch, queue, ce_unique_id, log):
    contact_string = cp_get(cp, batch, 'job_contact', ce_unique_id)

    if contact_string.endswith("jobmanager-%s" % batch):
        contact_string += "-%s" % queue

    if cp_getBoolean(cp, 'cream', 'enabled', False) and not \
           contact_string.endswith('cream-%s' % batch):
        log.warning('CREAM CE enabled, but contact string in config.ini '
                    'does not end with "cream-%s"' % batch)
		
    if contact_string.endswith('cream-%s' % batch):
        contact_string += "-%s" % queue
        if not contact_string.startswith('https://'):
            contact_string = 'https://' + contact_string

    return contact_string

def getHTPCInfo(cp, batch, queue, log):
    # return tuple: (non-Glue HTPC information, htpc_max_slots)
    #  where htpc_max_slots is the admin-provided "maximum number of slots per job"

    htpcInfo = ('__GIP_DELETEME', 1)  # defaults

    if not cp_getBoolean(cp, batch, 'htpc_enabled', False):
        log.info("HTPC is disabled for batch %s" % batch)
        return htpcInfo

    log.info("HTPC is enabled for batch %s" % batch)
    whitelist = cp_getList(cp, batch, 'htpc_queues', [])
    blacklist = cp_getList(cp, batch, 'htpc_blacklist_queues', [])

    log.debug("HTPC whitelist: %s; HTPC blacklist %s: " % (whitelist, blacklist))

    if '*' not in whitelist and queue not in whitelist:
        log.info("HTPC Queue %s not in whitelist" % queue)
        return htpcInfo

    if queue in blacklist:
        log.info("HTPC Queue %s in blacklist" % queue)
        return htpcInfo
        
    defaultRSL = cp_get(cp, batch, 'htpc_rsl', '')
    log.debug("HTPC DefaultRSL: %s" % defaultRSL)
    queueRSL = cp_get(cp, batch, 'htpc_rsl_%s' % queue, '')

    if not queueRSL:
        queueRSL = defaultRSL
        
    if not queueRSL:
        log.info("HTPC RSL not found for queue %s" % queue)
        return htpcInfo


    htpcMaxSlots = cp_getInt(cp, batch, 'htpc_max_slots', 1)
    if htpcMaxSlots < 2:
        log.info("HTPC max slots equal to 1 or not set!")
        return htpcInfo

    # acbr stuff?

    return ('HTPCrsl: %s' % queueRSL, htpcMaxSlots)
        
        
