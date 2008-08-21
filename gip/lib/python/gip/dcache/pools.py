
import re
import sys
import string
import traceback

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
        self.preciousKB = convertToKB( data.get('Precious', '0').split()[0] )
        self.removableKB = convertToKB( data.get('Removable', '0').split()[0] )
        self.reservedKB = convertToKB( data.get('Reserved', '0').split()[0] )
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

_pools_cache = []
def lookupPoolStorageInfo( connection, log ) :
    """
    Get pool storage info for all pools from the admin interface

    @param connection: Connection to the admin interface.
    @type connection: dCacheAdmin
    @param log: Log to use for this function.
    @type log: Logger
    """
    global _pools_cache
    if _pools_cache:
        return _pools_cache

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
    _pools_cache = list(listOfPools)
    return listOfPools

