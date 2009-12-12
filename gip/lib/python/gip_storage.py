
"""
Module for interacting with a dCache storage element.
"""

import os
import re
import gip_sets as sets
import stat
import statvfs

import gip_testing
from gip_common import cp_get, cp_getBoolean, cp_getInt, matchFQAN
from gip_logging import getLogger
from gip_sections import se
from gip.dcache.admin import connect_admin
from gip.dcache.pools import lookupPoolStorageInfo
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
        from psycopg2.extras import DictCursor #pylint: disable-msg=F0401
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
        p = psycopg2.connect(connectstring)
    except Exception:
        pgdb = __import__("pgdb")
        database = cp.get("dcache_config", "database")
        dbuser = cp.get("dcache_config", "dbuser")
        dbpasswd = cp.get("dcache_config", "dbpasswd")
        pghost = cp.get("dcache_config", "pghost")
        pgport = cp.get("dcache_config", "pgport")
        p = pgdb.connect(user = dbuser, password = dbpasswd, host='%s:%s' % \
            (pghost, pgport), database = database)

    return p

_defaultSE = None
def getDefaultSE(cp):
    global _defaultSE
    if _defaultSE:
        return _defaultSE
    default_se = cp_get(cp, "se", "name", "UNKNOWN")
    # if [se] name: ??? is "UNAVAILABLE" or not set, then try to get the 
    # default_se
    if default_se == "UNKNOWN" or default_se == "UNAVAILABLE":
        default_se = cp_get(cp, "se", "default_se", "UNAVAILABLE")
    # if it is still UNAVAILABLE or not set, check to see if the classic SE 
    # is being advertised and use that
    if default_se == "UNAVAILABLE" and cp_getBoolean(cp, "classic_se",
            "advertise_se", True):
        fallback_name = cp_get(cp, "site", "unique_name", "UNKNOWN") + \
            "_classicSE"
        default_se = cp_get(cp, "classic_se", "name", fallback_name)

    current_se = None
    for sect in cp.sections():
        if not sect.lower().startswith('se'):
            continue
        try:
            current_se = cp.get(sect, 'name')
        except:
            continue
        if cp_getBoolean(cp, sect, "default", False):
            _defaultSE = current_se
            return current_se
    if default_se == 'UNKNOWN' and current_se:
        _defaultSE = current_se
        return current_se
    _defaultSE = default_se
    return default_se

split_re = re.compile("\s*,?\s*")
def voListStorage(cp, section=None):
    """
    List of VOs which are allowed to access this storage element.

    @param cp: Configuration for this site
    @type cp: ConfigParser
    """
    log.debug("Listing storage VOs for section %s." % str(section))
    if section and section in cp.sections():
        if "allowed_vos" in cp.options(section):
            gip_common = __import__("gip_common")
            real_list = gip_common.voList(cp)
            lookup_vo = dict([(vo.lower(), vo) for vo in real_list])
            vo_set = sets.Set()
            for vo in split_re.split(cp.get(section, "allowed_vos")):
                vo = vo.lower()
                if vo in ['usatlas', 'uscms']:
                    vo = vo[2:]
                if vo in lookup_vo:
                    vo_set.add(lookup_vo[vo])
            log.debug("Valid VOs: %s" % ", ".join(vo_set))
            return vo_set
    try:
        autodetect = cp.getboolean("vo", "autodetect_storage_vos")
    except:
        autodetect = True
    if autodetect:
        gip_common = __import__("gip_common")
        return gip_common.voList(cp)
    vos = cp.get("vo", "storage_vos")
    vos = [i.strip() for i in vos.split(',')]

    # We do not use blacklist/whitelist since we've already listed explicitly
    # the VOs with storage_vos
    return vos

def getPath(cp, vo='', section='vo', classicSE=False):
    """
    Get the storage path for some VO.

    @param cp: Configuration for this site
    @type cp: ConfigParser
    @param vo: VO name (if vo='', then the default path will be given)
    """
    if classicSE:
        fallback1 = cp_get(cp, "osg_dirs", "data", "/UNKNOWN")
        fallback = cp_get(cp, section, "default", fallback1).replace("$VO", vo)\
            .replace("VONAME", vo)
    else:
        myvo = vo
        if not myvo:
            myvo = ''
        fallback = str(cp_get(cp, section, "default","/UNKNOWN")).\
            replace("$VO", myvo).replace("VONAME", myvo)
        if fallback == "/UNKNOWN" or fallback == 'UNAVAILABLE':
            fallback = str(cp_get(cp, section, "default_path","/UNKNOWN")).\
                replace("$VO", myvo).replace("VONAME", myvo)
    path = cp_get(cp, section, vo, fallback)
    return path

def getSESpace(cp, admin=None, gb=False, total=False, section=se):
    """
    Return the amount of space available at the SE.
    
    If se.dynamic_dcache=True, use dCache-based methods.
    Otherwise, use classic SE methods (do a df on the SE mounts).
    
    @param cp: Site configuration object
    @keyword admin: If a dCache provider, the dCache admin objects
    @keyword gb: Set to true to retun values in GB.
    @keyword total: Also return totals
    @return: used, free, total if total is True; otherwise, used, free.
      In GB if GB=True; otherwise, in KB.
    """
    if cp_getBoolean(cp, section, "dynamic_dcache", False):
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
    log.info("Calculating Classic SE space.")
    space_info = cp_get(cp, "classic_se", "space", None)
    if space_info:
        log.info("Using config file information.")
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
    log.info("Querying VFS stats for VO.")
    for vo in voListStorage(cp):
        path = getPath(cp, vo, classicSE=True, section='classic_se')
        # Skip fake paths
        if not os.path.exists(path):
            # Suppress warning message if we are in testing mode
            if not gip_testing.replace_command:
                log.warning("Skipping `df` of path %s because it does not" \
                    " exist." % (path))
            continue
        stat_info = os.stat(path)
        vfs_info = os.statvfs(path)
        device = stat_info[stat.ST_DEV]
        mount_info[device] = vfs_info
    # For each unique device, determine the free/total information from statvfs
    # results.
    for vfs in mount_info.values():
        dev_free = vfs[statvfs.F_FRSIZE] * vfs[statvfs.F_BAVAIL]
        dev_total = vfs[statvfs.F_FRSIZE] * vfs[statvfs.F_BLOCKS]
        dev_used = dev_total - dev_free
        used += dev_used
        free += dev_free
        tot += dev_total
    log.info("Resulting byte-values; used %i, free %i, total %i" % (used, free,
        tot))
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
    global dCacheSpace_cache # pylint: disable-msg=W0603
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

def getAccessProtocols(cp): #pylint: disable-msg=W0613
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

    """
    This class represents a logical StorageElement.
    
    The class implements the necessary functions for a generic SRM v2.2
    based storage element - however, it leaves many things blank as there's
    no way to determine space available, etc.  Provider implementors for SEs
    should subclass this and implement SE-specific functions. 
    """

    def __init__(self, cp, section="se"):
        self._cp = cp
        self._section = section

    def getSection(self):
        """
        Return the ConfigParser section this StorageElement is using.
        """
        return self._section

    def run(self):
        """
        Run whatever data-gathering activities which need to be done.
        
        For the base class, this is a no-op.
        """

    def getServiceVOs(self):
        """
        Return the list of VOs which are allowed to access this service.
        """
        return voListStorage(self._cp, self._section)

    def getServiceVersions(self):
        """
        Return the list of supported SRM versions.
        """
        return [2]

    def getAccessProtocols(self):
        """
        Stub function for providing access protocol information.

        Return a list of dictionaries. Each dictionary will
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

        For the base class, this just returns [].
        """
        mount_point = cp_get(self._cp, self._section, 'mount_point', None)
        if None:
            return []
        return [{'protocol': 'file', 'hostname': 'POSIX.example.com', 'port': '1234',
            'version': '1.0.0'}]

    def hasSRM(self):
        """
        Return True if there is a SRM endpoint present on this SE.
        """
        return cp_getBoolean(self._cp, self._section, "srm_present", True)

    split_re = re.compile("\s*,?\s*")
    def getSRMs(self):
        """
        Return a list of dictionaries containing information about the SRM
        endpoints.
        
        Each dictionary must have the following keys:
           - acbr
           - status
           - version
           - endpoint
           - name
           
        The base class implementation uses the following configuration entries
        (default value in parenthesis)
           - se.srm_host (default: UNKNOWN.example.com)
           - se.srm_version (2.2.0)
           - se.srm_port (8443)
           - se.srm_endpoint 
             (httpg://(se.srm_host):(se.srm_port)/srm/managerv2)
        Any of the above may be a comma- or space-separated list.
        """
        srmhost_str = cp_get(self._cp, self._section, "srm_host",
            "UNKNOWN.example.com")
        srmhosts = self.split_re.split(srmhost_str)
        version_str = cp_get(self._cp, self._section, "srm_version", "2")
        versions = self.split_re.split(version_str)
        while len(versions) <= len(srmhosts):
            versions.append(versions[-1])
        port_str = cp_get(self._cp, self._section, "srm_port", "8443")
        ports = self.split_re.split(port_str)
        while len(ports) <= len(srmhosts):
            ports.append(ports[-1])
        default_endpoints = []
        for idx in range(len(srmhosts)):
            if versions[idx].find('2') >= 0:
                default_endpoint = 'httpg://%s:%i/srm/managerv2' % \
                    (srmhosts[idx], int(ports[idx]))
            else:
                default_endpoint = 'httpg://%s:%i/srm/managerv1' % \
                    (srmhosts[idx], int(ports[idx]))
            default_endpoints.append(default_endpoint)
        endpoint_str = cp_get(self._cp, self._section, "srm_endpoint", None)
        if endpoint_str:
            endpoints = self.split_re.split(endpoint_str)
        else:
            endpoints = default_endpoints

        srms = []
        idx = 0
        srmname_re = re.compile("://(.*?):")
        for endpoint in endpoints:
            acbr_tmpl = '\nGlueServiceAccessControlRule: %s\n' \
                'GlueServiceAccessControlRule: VO:%s'
            acbr = ''
            vos = voListStorage(self._cp, self._section)
            for vo in vos:
                acbr += acbr_tmpl % (vo, vo)
       
            name = endpoint
            m = srmname_re.search(endpoint)
            if m:
                name = m.groups()[0]

            info = {'acbr': acbr[1:],
                    'status': 'OK',
                    'version': versions[idx],
                    'endpoint': endpoint,
                    'name': name,
                   }
            idx += 1
            srms.append(info)

        return srms
 
    def getName(self):
        """
        Return the name of the SE.
        
        The base class uses the value of se.name in the configuration object.
        """
        return cp_get(self._cp, self._section, 'name', 'UNKNOWN')

    def getUniqueID(self):
        """
        Return the unique ID of the SE.
        
        The base class uses the value of se.unique_name (defaults to se.name)
        in the configuraiton object.
        """
        return cp_get(self._cp, self._section, 'unique_name', 
            cp_get(self._cp, self._section, 'name', 'UNKNOWN'))

    def getStatus(self):
        """
        Return the status of the SE.
        
        The base classes uses the value of se.status (defaults to Production)
        in the configuration object.
        """
        return cp_get(self._cp, self._section, "status", "Production")

    def getImplementation(self):
        """
        Return the implementation name for this SE.
        
        The base class uses the value of se.implementation (defaults to 
        UNKNOWN) in the configuration object.
        """
        return cp_get(self._cp, self._section, "implementation", "UNKNOWN")

    def getVersion(self):
        """
        Return a version string for this SE.
        
        The base class uses the value of se.version (defaults to UNKNOWN)
        in the configuration object.
        """
        version = cp_get(self._cp, self._section, "version", "UNKNOWN")
        return version

    def getSESpace(self, gb=False, total=False):
        """
        Returns information about the SE disk space.
        
        @see: getSESpace (module-level implementation)
        """
        return getSESpace(self._cp, total=total, gb=gb)

    def hasTape(self):
        """
        Returns true if the SE has an attached tape system.
        """
        return seHasTape(self._cp)

    def getSETape(self):
        """
        Retrieve the freespace information from the tape systems.
        
        @see: getSETape (module level implementation)
        """
        return getSETape(self._cp)

    def getSEArch(self):
        """
        Returns the SE architecture.
        
        This is an enumeration; the possible values are "tape",
        "multi-disk", "disk", or "other".
        
        The base class makes an educated guess based upon the implementation
        name and the return value of hasTape. 
        """
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
        path = self.getPathForSA(space=None, section=self._section)
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
            used, available, total = self.getSESpace(total=True)
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
        """
        Return a list of VOInfo dictionaries.
        
        Each dictionary must have the following keys:
           - voInfoID
           - name
           - path
           - tag
           - acbr
           - saLocalID
        
        """
        voinfos = []
        for sa_info in self.getSAs():
            vos = sa_info.get("vos", sa_info.get('name', None))
            if not vos:
                continue
            for vo in vos:
                myid = '%s:default' % vo
                path = self.getPathForSA(space=None, vo=vo, section=self._section)
                acbr = 'GlueVOInfoAccessControlBaseRule: %s' % vo
                info = {'voInfoID': myid,
                        'name': myid,
                        'path': path,
                        'tag': '__GIP_DELETEME',
                        'acbr': acbr,
                        'saLocalID': sa_info.get('saLocalID', 'UNKNOWN'),
                       }
                voinfos.append(info)
        return voinfos

    def getVOsForSpace(self, space): #pylint: disable-msg=W0613
        """
        Given a certain space, return a list of 
        """
        return voListStorage(self._cp, self._section)

    def getPathForSA(self, space=None, vo=None, return_default=True,
            section='se'):
        """
        Return a path appropriate for a VO and space.

        Based upon the configuration info and the VO/space requested, determine
        a path they should use.

        This function tries to find option dcache.space_<space>_path; it parses
        this as a comma-separated list of VO:path pairs; i.e., 
            space_CMS_path=cms:/dev/null, atlas:/pnfs/blah

        If that does not provide a match and return_default is true, then it 
        will look for dcache.space_<space>_default_path and return that.

        If that is not there and return_default is true, it will use the 
        standard getPath from gip_storage.

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
        log.debug("Get path for SA %s, vo %s, section %s." % (space, vo, section))
        default_path = cp_get(self._cp, self._section, 
            "space_%s_default_path" % space, None)
        if not default_path:
            if self._section == 'se':
                default_path = getPath(self._cp, vo)
            else:
                default_path = getPath(self._cp, vo, section=self._section)
        log.debug("My default path: %s" % default_path)
        if not vo:
            return default_path
        vo_specific_paths = cp_get(self._cp, section, "space_%s_path" % space,
           None)
        if vo_specific_paths == None:
            vo_specific_paths = cp_get(self._cp, section, "vo_dirs", None)
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


