"""
Common functions for GIP batch system providers and plugins.
"""

from gip_common import cp_getBoolean, cp_get
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
    gramVersion = '\n' + 'GlueCEInfoGRAMVersion: 2.0'
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
