
"""
Module for interacting with a dCache storage element.
"""

import os
import re
import sys
import sets
import stat
import string
import statvfs
import traceback

from gip_common import getLogger, cp_get, cp_getBoolean, cp_getInt
from gip_sections import se
from gip.dcache.admin import connect_admin
from gip.dcache.pools import convertToKB, lookupPoolStorageInfo
log = getLogger("GIP.Storage")

def execute(p, command, bind_vars=None):
    """
    Given a Postgres connection, execute a SQL statement.

    @param p: Postgres connection, as returned by L{connect}
    @type p: psycopg2.Connection
    @param command: SQL statement
    @param bind_vars: Bind vars for B{command}, if any.
    @returns: All resulting rows.
    """
    try:
        from psycopg2.extras import DictCursor
        curs = p.cursor(cursor_factory=DictCursor)
    except:
        curs = p.cursor()
    if bind_vars != None:
        curs.execute(command, bind_vars)
    else:
        curs.execute(command)
    rows = curs.fetchall()
    return rows

def connect(cp):
    """
    Connect to the SRM database based upon the parameters in the passed
    config file.

    @param cp: Site configuration
    @type cp: ConfigParser
    @returns: Connection to the SRM database.
    @rtype: psycopg2.Connection or pgdb.Connection
    """
    try:
        psycopg2 = __import__("psycopg2")
        database = cp.get("dcache_config", "database")
        dbuser = cp.get("dcache_config", "dbuser")
        dbpasswd = cp.get("dcache_config", "dbpasswd")
        pghost = cp.get("dcache_config", "pghost")
        pgport = cp.get("dcache_config", "pgport")
        connectstring = "dbname=%s user=%s password=%s host=%s port=%s" % \
            (database, dbuser, dbpasswd, pghost, pgport)
        p=psycopg2.connect(connectstring)
    except Exception, e:
        pgdb = __import__("pgdb")
        database = cp.get("dcache_config", "database")
        dbuser = cp.get("dcache_config", "dbuser")
        dbpasswd = cp.get("dcache_config", "dbpasswd")
        pghost = cp.get("dcache_config", "pghost")
        pgport = cp.get("dcache_config", "pgport")
        p=pgdb.connect(user=dbuser, password=dbpasswd, host='%s:%s' % \
            (pghost, pgport), database=database)

    return p

def voListStorage(cp):
    """
    List of VOs which are allowed to access this storage element.

    @param cp: Configuration for this site
    @type cp: ConfigParser
    """
    try:
        autodetect = cp.getboolean("vo", "autodetect_storage_vos")
    except:
        autodetect = True
    if autodetect:
        gip_common = __import__("gip_common")
        return gip_common.voList(cp)
    vos = cp.get("vo", "storage_vos")
    vos = [i.strip() for i in vos.split(',')]
    blacklist = cp_get(cp, "vo", "vo_blacklist", "").split(',')
    blacklist = [i.strip() for i in blacklist]
    whitelist = cp_get(cp, "vo", "vo_whitelist", "").split(',')
    whitelist = [i.strip() for i in whitelist]
    for vo in whitelist:
        if vo not in vos:
            vos.append(vo)
    refined = []
    for vo in vos:
        if vo not in blacklist:
            refined.append(vo)
    return refined

def getPath(cp, vo='', section='vo', classicSE=False):
    """
    Get the storage path for some VO.

    @param cp: Configuration for this site
    @type cp: ConfigParser
    @param vo: VO name (if vo='', then the default path will be given)
    """
    if classicSE:
        fallback1 = cp_get(cp, "osg_dirs", "data", "/UNKNOWN")
        fallback = cp_get(cp, section, "default", fallback1).replace("$VO", vo)
    else:
        myvo = vo
        if not myvo:
            myvo = ''
        fallback = cp_get(cp, section, "default","/UNKNOWN").replace("$VO",myvo)
    path = cp_get(cp, section, vo, fallback)
    return path

def getSESpace(cp, admin=None, gb=False, total=False):
    if cp_getBoolean(cp, se, "dynamic_dcache", False):
        return getdCacheSESpace(cp, admin, gb, total)
    else:
        return getClassicSESpace(cp, gb=gb, total=total)

def getClassicSESpace(cp, gb=False, total=False):
    """
    Get the total amount of the locally available space.  By default, return
    the information in kilobytes.

    @param cp: Site configuration
    @type cp: ConfigParser
    @keyword gb: If True, then return the results  in GB, not KB.
    @keyword total: If True, also return the total amount of space in the SE.
    @returns: Returns the used space, free space.  If C{total=true}, also 
        return the total space.  If C{gb=True}, return the numbers in GB;
        otherwise the numbers are in kilobytes.
    """
    space_info = cp_get(cp, "classic_se", "space", None)
    if space_info:
        # Assume that the space reported is in KB
        used, free, tot = eval(space_info, {}, {})
        # Divide by 1000**2 to go from KB to GB
        if gb:
            used /= 1000**2
            free /= 1000**2
            tot /= 1000**2
        if total:
            return used, free, tot
        return used, free, tot
    used, free, tot = 0, 0, 0
    mount_info = {}
    # First, find out all the storage paths for the supported VOs
    # Uses statvfs to find out the mount info; stat to find the device ID
    for vo in voListStorage(cp):
        path = getPath(cp, vo)
        # Skip fake paths
        if not os.path.exists(path):
            continue
        stat_info = os.stat(path)
        vfs_info = os.statvfs(path)
        device = stat_info[stat.ST_DEV]
        mount_info[device] = vfs_info
    # For each unique device, determine the free/total information from statvfs
    # results.
    for dev, vfs in mount_info.items():
        dev_free = vfs[statvfs.F_FRSIZE] * vfs[statvfs.F_BAVAIL]
        dev_total = vfs[statvfs.F_FRSIZE] * vfs[statvfs.F_BLOCKS]
        dev_used = dev_total - dev_free
        used += dev_used
        free += dev_free
        tot += dev_total
    if gb: # Divide by 1000^2.  Results in a number in MB
        used /= 1000000L
        free /= 1000000L
        tot /= 1000000L
    # Divide by 1000 to get KB or GB
    used /= 1000
    free /= 1000
    tot /= 1000
    if total:
        return used, free, tot
    return used, free

dCacheSpace_cache = None
def getdCacheSESpace(cp, admin=None, gb=False, total=False):
    """
    Get the total amount of space available in a dCache instance.  By default,
    return the information in Kilobytes.

    @param cp: Site configuration
    @type cp: ConfigParser
    @keyword admin: If set, reuse this admin interface instead of making a new
        connection.
    @keyword gb: If True, then return the results  in GB, not KB.
    @keyword total: If True, also return the total amount of space in the SE.
    @returns: Returns the used space, free space.  If C{total=true}, also 
        return the total space.  If C{gb=True}, return the numbers in GB;
        otherwise the numbers are in kilobytes.
    """
    global dCacheSpace_cache
    if admin == None:
        admin = connect_admin(cp)
    if not dCacheSpace_cache:
        pools = lookupPoolStorageInfo(admin, log)
        used = 0L # In KB
        free = 0L # In KB
        tot  = 0L # In KB
        for pool in pools:
            used += pool.usedSpaceKB
            free += pool.freeSpaceKB
            tot  += pool.totalSpaceKB
        dCacheSpace_cache = used, free, tot
    else:
        used, free, tot = dCacheSpace_cache
    if gb:
        used /= 1000000L
        free /= 1000000L
        tot  /= 1000000L
    if total:
        return used, free, tot
    return used, free

def seHasTape(cp):
    """
    Determine if the SE has tape information

    @param cp: Site configuration
    @type cp: ConfigParser
    @returns: True if there is tape info; False otherwise.
    """
    if cp.has_section("tape_info"):
        return True
    return False

def getSETape(cp, vo="total"):
    """
    Get the amount of tape available; numbers are in kilobytes.

    If there is no tape information available, everything is 0.

    @param cp: Site configuration
    @type cp: ConfigParser
    @keyword vo: The VO information to consider; to get the aggregate info, use
        C{vo="total"}
    @returns: The used space, free space, and total space on tape in kilobytes.
    """
    # Load up the tape statistics from the config file
    if cp.has_option("tape_info", vo):
        un, fn = [long(i.strip()) for i in cp.get("tape_info", vo).split(',')]
        tn = long(un) + long(fn)
    else:
        #Or, if it's not there, ignore it!
        tn = 0
        un = 0
        fn = 0
    return un, fn, tn

def getSEVersion(cp, admin=None):
    """
    Get the version info from the dCache system.

    @param cp: A config parser object which holds the dCache login information
    @type cp: ConfigParser
    @keyword admin: An instance of the L{dCacheAdmin} interface.  If it is None,
        then `cp` will be used to log in to the admin interface.
    @return: The dCache version number; UNKNOWN if it can't be determined.
    """
    if admin == None:
        admin = connect_admin(cp)
    pools = admin.execute("PoolManager", "cm ls")
    pool = None
    for line in pools.split('\n'):
        pool = line.split('=')[0]
        break
    if pool == None:
        log.warning("No pools found attached to dCache.")
        return "UNKNOWN"
    pool_info = admin.execute(pool, "info")
    version = None
    for line in pool_info.split('\n'):
        line_info = line.split()
        if line_info[0].strip() == 'Version':
            version = line_info[2].strip()
            break
    if version == None:
        log.warning("Unable to parse version info from pool %s." % str(pool))
        return "UNKNOWN"
    version_re = re.compile("(.*)-(.*)-(.*)-(.*)-(.*)\((.*)\)")
    m = version_re.match(version)
    if m:
        kind, major, minor, bugfix, patch, revision = m.groups()
        if kind != "production":
            return "%s.%s.%s-%s (r%s), %s" % (major, minor, bugfix, patch,
                revision, kind)
        else:
            return "%s.%s.%s-%s" % (major, minor, bugfix, patch)
    else:
        return version

def getAccessProtocols(cp):
    """
    Stub function for providing access protocol information.

    Eventually, this will return a list of dictionaries.  Each dictionary will
    have the following keys with reference to an access endpoint:
       
       - protocol
       - hostname
       - port
    
    Optionally, the following keys may be included (default in parenthesis):
       
       - capability (file transfer)
       - maxStreams (1)
       - securityinfo (none)
       - version (UNKNOWN)
       - endpoint (<protocol>://<hostname>:<port>)

    Currently, this just returns []
    """
    return []

class StorageElement(object):

    def __init__(self, cp):
        self._cp = cp

    def run(self):
        pass

    def getServiceVOs(self):
        return voListStorage(self._cp)

    def getServiceVersions(self):
        return [2]

    def getAccessProtocols(cp):
        """
        Stub function for providing access protocol information.

        Eventually, this will return a list of dictionaries. Each dictionary will
        have the following keys with reference to an access endpoint:
       
           - protocol
           - hostname
           - port
    
        Optionally, the following keys may be included (default in parenthesis):
       
           - capability (file transfer)
           - maxStreams (1)
           - securityinfo (none)
           - version (UNKNOWN)
           - endpoint (<protocol>://<hostname>:<port>)

        Currently, this just returns []
        """
        return []

    def hasSRM(self):
        return cp_getBoolean(self._cp, "se", "srm_present", True)

    def getSRMs(self):

        srmname = cp_get(self._cp, "se", "srm_host", "UNKNOWN.example.com")
        version = cp_get(self._cp, "se", "srm_version", "2")
        port = cp_getInt(self._cp, "se", "srm_port", 8443)
        if version.find('2') >= 0:
            default_endpoint = 'httpg://%s:%i/srm/managerv2' % \
                (srmname, int(port))
        else:
            default_endpoint = 'httpg://%s:%i/srm/managerv1' % \
                (srmname, int(port))
        endpoint = cp_get(self._cp,"se", "srm_endpoint", default_endpoint)

        acbr_tmpl = '\nGlueServiceAccessControlRule: %s\n' \
            'nGlueServiceAccessControlRule: VO:%s'
        acbr = ''
        vos = voListStorage(self._cp)
        for vo in vos:
            acbr += acbr_tmpl % (vo, vo)
       
        info = {'acbr': acbr[1:],
                'status': 'OK',
                'version': version,
                'endpoint': endpoint,
                'name': srmname,
               }

        return [info]
 
    def getName(self):
        return cp_get(self._cp, 'se', 'name', 'UNKNOWN')

    def getUniqueID(self):
        return cp_get(self._cp, 'se', 'unique_name', 'UNKNOWN')

    def getStatus(self):
        return cp_get(self._cp, "se", "status", "Production")

    def getImplementation(self):
        return cp_get(self._cp, "se", "implementation", "UNKNOWN")

    def getVersion(self):
        version = cp_get(self._cp, "se", "version", "UNKNOWN")
        return version

    def getSESpace(self, gb=False, total=False):
        return getSESpace(self._cp, total=total, gb=gb)

    def hasTape(self):
        return seHasTape(self._cp)

    def getSETape(self):
        return getSETape(self._cp)

    def getSEArch(self):
        implementation = self.getImplementation()
        if self.hasTape():
            arch = "tape"
        elif implementation=='dcache' or \
                implementation.lower() == 'bestman/xrootd':
            arch = 'multi-disk'
        elif implementation.lower() == 'bestman':
            arch = 'disk'
        else:
            arch = 'other'
        return arch

    def getSAs(self):
        """
        Return a list of storage areas at this site.

        For each storage area, we have a dictionary with the following keys:

        Required:
           - saLocalID
           - path
           - acbr

        Optional (default):
           - root (/)
           - filetype (permanent)
           - saName (saLocalID)
           - totalOnline; in GB (0)
           - usedOnline; in GB (0)
           - freeOnline; in GB (0)
           - reservedOnline; in GB (0)
           - totalNearline; in GB (0)
           - usedNearline; in GB (0)
           - freeNearline; in GB (0)
           - reservedNearline; in GB (0)
           - retention (replica)
           - accessLatency (online)
           - expiration (neverExpire)
           - availableSpace; in KB (0)
           - usedSpace; in KB (0)

        @returns: List of dictionaries containing SA info.
        """
        path = self.getPathForSA(space=None)
        vos = self.getVOsForSpace(None)
        sa_vos = sets.Set()
        for vo in vos:
            sa_vos.add(vo)
            if not vo.startswith('VO'):
                sa_vos.add('VO: %s' % vo)
        sa_vos = list(sa_vos)
        sa_vos.sort()
        acbr = '\n'.join(['GlueSAAccessControlBaseRule: %s' % i \
            for i in sa_vos])
        try:
            used, available, total = getSESpace(self._cp, total=True)
        except Exception, e:
            log.error("Unable to get SE space: %s" % str(e))
            used = 0
            available = 0
            total = 0
        info = {'saLocalID': 'default',
                'path': path,
                'saName': 'Default Storage Area',
                "totalOnline"      : int(round(total/1000**2)),
                "usedOnline"       : int(round(used/1000**2)),
                "freeOnline"       : int(round(available/1000**2)),
                "availableSpace"   : available,
                "usedSpace"        : used,
                "vos"              : vos,
                'acbr'             : acbr,
               }
        return [info]

    def getVOInfos(self):
        voinfos = []
        for sa_info in self.getSAs():
            vos = sa_info.get("vos", sa_info.get('name', None))
            if not vos:
                continue
            for vo in vos:
                id = '%s:default' % vo
                path = self.getPathForSA(space=None, vo=vo)
                acbr = 'GlueVOInfoAccessControlBaseRule: %s' % vo
                info = {'voInfoID': id,
                        'name': id,
                        'path': path,
                        'tag': 'Not A Space Reservation',
                        'acbr': acbr,
                        'saLocalID': sa_info.get('saLocalID', 'UNKNOWN'),
                       }
                voinfos.append(info)
        return voinfos

    def getVOsForSpace(self, space):
        return voListStorage(self._cp)

    def getPathForSA(self, space=None, vo=None, return_default=True,
            section='se'):
        """
        Return a path appropriate for a VO and space.

        Based upon the configuration info and the VO/space requested, determine
        a path they should use.

        This function tries to find option dcache.space_<space>_path; it parses
        this as a comma-separated list of VO:path pairs; i.e., 
            space_CMS_path=cms:/dev/null, atlas:/pnfs/blah

        If that does not provide a match and return_default is true, then it will
        look for dcache.space_<space>_default_path and return that.

        If that is not there and return_default is true, it will use the standard
        getPath from gip_storage.

        If return_default is true, this is guaranteed to return a non-empty
        string; if return_default is false, then this might through a ValueError
        exception.
        
        @param cp: Site config object
        @param space: The name of the space to determine the path for.
        @param vo: The name of the VO which will be using this space; None for
            the default information.
        @kw return_default: Set to True if you want this function to return the
            default path if it cannot find a VO-specific one.
        @returns: A path string; raises a ValueError if return_default=False
        """
        default_path = cp_get(self._cp, section, "space_%s_default_path" % space,
            None)
        if not default_path:
            default_path = getPath(self._cp, vo)
        if not vo:
            return default_path
        vo_specific_paths = cp_get(self._cp, section, "space_%s_path" % space,
           None)
        vo_specific_path = None
        if vo_specific_paths:
            for value in vo_specific_paths.split(','):
                value = value.strip()
                info = value.split(':')
                if len(info) != 2:
                    continue
                fqan, path = info
                if matchFQAN(vo, fqan):
                    vo_specific_path = path
                    break
        if not vo_specific_path and return_default:
            return default_path
        elif not vo_specific_path:
            raise ValueError("Unable to determine path for %s!" % vo)
        return vo_specific_path


