
"""
Module for interacting with a dCache storage element.
"""

import string
import traceback
import sys
import re

from gip_common import getLogger
import dCacheAdmin

log = getLogger("GIP.Storage")

def connect_admin(cp):
    """
    Connect to the site's admin interface.

    @param cp: Configuration of the site.
    @type cp: ConfigParser
    """
    info = {'Interface':'dCache'}
    info['AdminHost'] = cp.get("dcache_admin", "hostname")
    try:
        info['Username'] = cp.get("dcache_admin", "username")
    except:
        pass
    try:
        info['Cipher'] = cp.get("dcache_admin", "cipher")
    except:
        pass
    try:
        info['Port'] = cp.get("dcache_admin", "port")
    except:
        pass
    try:
        info['Password'] = cp.get("dcache_admin", "password")
    except:
        pass
    try:
        info['Protocol'] = cp.get("dcache_admin", "protocol")
    except:
        pass
    try:
        timeout = cp.getint("dcache_admin", "timeout")
    except:
        timeout = 5
    return dCacheAdmin.Admin(info, timeout)

def execute(p, command, bind_vars=None):
    """
    Given a Postgres connection, execute a SQL statement.

    @param p: Postgres connection, as returned by L{connect}
    @type p: psycopg2.Connection
    @param command: SQL statement
    @param bind_vars: Bind vars for B{command}, if any.
    @returns: All resulting rows.
    """
    from psycopg2.extras import DictCursor
    curs = p.cursor(cursor_factory=DictCursor)
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
    @rtype: psycopg2.Connection
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
        raise

    return p

def voListStorage(cp):
    """
    List of VOs which are allowed to access this storage element.

    @param cp: Configuration for this site
    @type cp: ConfigParser
    """
    if cp.getboolean("vo", "autodetect_storage_vos"):
        gip_common = __import__("gip_common")
        return gip_common.voList(cp)
    vos = cp.get("vo", "storage_vos")
    vos = [i.strip() for i in vos.split(',')]
    blacklist = cp.get("vo", "vo_blacklist").split(',')
    blacklist = [i.strip() for i in blacklist]
    whitelist = cp.get("vo", "vo_whitelist").split(',')
    whitelist = [i.strip() for i in whitelist]
    for vo in whitelist:
        if vo not in vos:
            vos.append(vo)
    refined = []
    for vo in vos:
        if vo not in blacklist:
            refined.append(vo)
    return refined

def getPath(cp, vo):
    """
    Get the storage path for some VO.

    @param cp: Configuration for this site
    @type cp: ConfigParser
    @param vo: VO name (if vo='', then the default path will be given)
    """
    if cp.has_option("vo", vo):
        path = cp.get("vo", vo)
    else:
        path = cp.get("vo", "default").replace("$VO", vo)
    return path

# The next three definitions are taken from the Gratia storage probe

def convertToKB( valueString ) : 
    """ 
    This function translates file sizes from the units specified to kilobytes.
    The unit specifiers can be in uppercase or lowercase. Acceptable unit
    specifiers are g m k b. If no specifier is provided, bytes is assumed.
    E.g., 10G is translated to 10485760.

    1048576 is assumed to be in bytes and is translated to 1024.

    @param valueString: Size to be converted to kilobytes.
    """ 
    result = re.split( '(\d+)', string.lower( valueString ), 1 )
    val = long( result[1] )
    if len( result ) == 2 :
        # Convert bytes to kilobytes
        return (val + 1023 )/ 1024
    if len( result ) == 3 :
        result[2] = result[2].strip()
        if result[2] == 'g' : # Convert gigabytes to kilobytes
            return val * 1024 * 1024
        if result[2] == 'm' : # Convert megabytes to kilobytes
            return val * 1024
        if result[2] == 'k' : # No conversion needed
            return val
        if result[2] == 'b' or result[2] == '' :
            # Convert bytes to kilobytes
            return ( val + 1023 ) / 1024
    raise Exception( 'unknown size qualifier "' + str( result[2] ) + '"' )

class Pool :
    """ 
    This is a container class that parses a pool info object and caches
    the information about a dCache Pool that is required by Gratia,
    until we are ready to send it.
    """ 
    def __init__( self, poolName, poolInfo ) :

        data = {}
        for line in string.split( poolInfo, '\n' ) :
            y = string.split( line, ':' )
            if len( y ) > 1 :
                data[ y[0].strip() ] = y[1].strip()
        self.poolName = poolName
        # The total is frequently given as [0-9]+G to signify gigabytes
        self.totalSpaceKB = convertToKB( data[ 'Total' ] )
        self.usedSpaceKB = convertToKB( ( data[ 'Used' ].split() )[0] )
        self.freeSpaceKB = convertToKB( ( data[ 'Free' ].split() )[0] )
        self.type = string.lower( data[ 'LargeFileStore' ] )
        self.pnfsRoot = data[ 'Base directory' ]

    def __repr__( self ) :
        # Make a string representation of the pool data.
        return self.poolName + \
               ', totalKB = ' + str( self.totalSpaceKB ) + \
               ', usedKB = ' + str( self.usedSpaceKB ) + \
               ', freeKB = ' + str( self.freeSpaceKB ) + \
               ', type = ' + self.type + \
               ', pnfs root = ' + self.pnfsRoot

def lookupPoolStorageInfo( connection, log ) :
    """
    Get pool storage info for all pools from the admin interface

    @param connection: Connection to the admin interface.
    @type connection: dCacheAdmin
    @param log: Log to use for this function.
    @type log: Logger
    """
    listOfPools = []
    # get a list of pools
    # If this raises an exception, it will be caught in main.
    # It is a fatal error...
    pooldata = connection.execute( 'PoolManager', 'cm ls' )
                    
    # for each pool get the vital statistics about capacity and usage
    defPoolList = pooldata.splitlines()
    for poolStr in defPoolList :
        poolName = poolStr.split( '={', 1 )[0]
        if string.strip( poolName ) == '' :
            continue # Skip empty lines.
        log.debug( 'found pool:' + str( poolName ) )
        try :
            poolinfo = connection.execute( poolName, 'info -l' )
            if poolinfo != None :
                listOfPools.append( Pool( poolName, poolinfo ) )
            else :
                log.error( 'Error doing info -l on pool ' + str( poolName ) )
        except :
            tblist = traceback.format_exception( sys.exc_type,
                                                 sys.exc_value,
                                                 sys.exc_traceback )
            log.warning( 'Got exception:\n\n' + "".join( tblist ) + \
                         '\nwhile doing "info -l" for pool ' + \
                         str( poolName ) + '.\nIgnoring this pool.' )
    
    return listOfPools


def getSESpace(cp, admin=None, gb=False, total=False):
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
    if admin == None:
        admin = connect_admin(cp)
    pools = lookupPoolStorageInfo(admin, log)
    used = 0L # In KB
    free = 0L # In KB
    tot  = 0L # In KB
    for pool in pools:
        used += pool.usedSpaceKB
        free += pool.freeSpaceKB
        tot  += pool.totalSpaceKB
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
        return "UNKNOWN"
    pool_info = admin.execute(pool, "info")
    version = None
    for line in pool_info.split('\n'):
        line_info = line.split()
        if line_info[0].strip() == 'Version':
            version = line_info[2].strip()
            break
    if version == None:
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

