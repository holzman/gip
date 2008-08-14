
"""
Module for interacting with a dCache storage element.
"""

import os
import re
import sys
import stat
import string
import statvfs
import traceback

from gip_common import getLogger, cp_get, cp_getBoolean
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

