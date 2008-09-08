
"""
Interact with the SrmSpaceManager and the PoolManager to calculate the 
important info about available space in dCache
"""

import re
import sets

# dCache imports
import pools as pools_module
import admin
import parsers

# GIP imports
from gip_common import getLogger, VoMapper, matchFQAN, cp_get
from gip_storage import getSETape, voListStorage, getPath as getStoragePath

log = getLogger("GIP.dCache.SA")

PoolManager = 'PoolManager'
SrmSpaceManager = 'SrmSpaceManager'

def calculate_spaces(cp, admin):
    """
    Determine the storage areas attached to this dCache.

    This returns two lists.  The first list, sas, is a list of dictionaries
    which contain the key-value pairs needed to fill out the GlueSA object.

    The second list, vos, is a list of dictionaries which contain the key-value
    pairs needd to fill in the GlueVOInfo object.

    @param cp: ConfigParser object
    @param admin: Admin interface to dCache
    @returns: sas, vos (see above description of return values.
    """
    # If SrmSpaceManager isn't running, this will cause an exception.
    # Catch it and pretend we just have no reservations or link groups
    try:
        space_output = admin.execute(SrmSpaceManager, 'ls')
        resv, lg = parsers.parse_srm_space_manager(space_output)
    except:
        resv = []
        lg = []

    # Get the pool information
    psu_output = admin.execute(PoolManager, 'psu dump setup')
    pgroups, lgroups, links, link_settings, pools = \
        parsers.parse_pool_manager(psu_output)
    listOfPools = pools_module.lookupPoolStorageInfo(admin, \
        getLogger("GIP.dCache.Pools"))
    pm_info = admin.execute(PoolManager, 'info')
    can_stage = pm_info.find('Allow staging : on') >= 0
    can_p2p = pm_info.find('Allow p2p : on') >= 0

    # Some post-parsing: go from list of pools to dictionary by pool name
    pool_info = {}
    pool_objs = {}
    for pool in listOfPools:
        pool_info[pool.poolName] = pool.totalSpaceKB
        pool_objs[pool.poolName] = pool
    for pool in pools:
        if pool not in pool_info:
            pool_info[pool] = 0


    # In order to make sure we don't have overlapping spaces, we remove the pool
    # from the pools list in order to record ones we already account for.

    # Build the map from link group to pools
    lgroups_to_pools = {}
    for lgroup, assc_links in lgroups.items():
        cur_set = sets.Set()
        lgroups_to_pools[lgroup] = cur_set
        for link in assc_links:
            for pgroup in links[link]:
                for pool in pgroups[pgroup]:
                    cur_set.add(pool)
                    pools.remove(pool)

    # Ensure already-seen pools are not in the remaining pool groups
    for pgroup, pg_set in pgroups.items():
        pg_set.intersection_update(pools)

    def cmp(x, y):
        "Sort pool groups by total size"
        return sum([pool_info[i] for i in pgroups[x]]) < \
               sum([pool_info[i] for i in pgroups[y]])

    pgroup_list = pgroups.keys()
    pgroup_list.sort(cmp=cmp)

    sas = []
    vos = []
    # Build a SA from each link group
    for lgroup, lgpools in lgroups.items():
        lg_info = None
        for l in lg:
            if l['name'] == lgroup:
                lg_info = l
                break
        if not lg_info:
            continue
        sa = calculate_space_from_linkgroup(cp,lg_info, [pool_objs[i] for i in \
            lgpools if i in pool_objs])
        sas.append(sa)
        voinfos = calculate_voinfo_from_lg(cp, lg_info, resv)
        vos.extend(voinfos)

    # Build a SA from each nontrivial pool group
    # Start with the largest and work our way down.
    for pgroup in pgroup_list:
        pg_pools = pgroups[pgroup]
        del pgroups[pgroup]
        for pg2, pg2_pools in pgroups.items():
            pg2_pools.difference_update(pg_pools)
        my_pool_objs = [pool_objs[i] for i in pg_pools if i in pool_objs]
        if not my_pool_objs:
            continue
        sa = calculate_space_from_poolgroup(cp, pgroup, my_pool_objs, admin,
            links, link_settings, allow_staging=can_stage, allow_p2p=can_p2p)
        sas.append(sa)
        voinfos = calculate_voinfo_from_pgroup(cp, pgroup)
        vos.extend(voinfos)

    return sas, vos

def calculate_space_from_poolgroup(cp, pgroup, pools, admin, links, \
        link_settings, allow_staging=False, allow_p2p=False):
    saLocalID = '%s:poolgroup' % pgroup
    seUniqueID = cp.get('se', 'unique_name')
    myLinks = sets.Set()
    for link, pgroups in links.items():
        if pgroup in pgroups:
            myLinks.add(link)
    or_func = lambda x, y: x or y
    can_write = reduce(or_func, [link_settings[i]['write']>0 for i in myLinks],
        False)
    can_read = reduce(or_func, [link_settings[i]['read']>0 for i in myLinks],
        False)
    can_p2p = reduce(or_func, [link_settings[i]['p2p']>0 for i in myLinks],
        False) and allow_p2p
    can_stage = reduce(or_func, [link_settings[i]['cache']>0 for i in myLinks],
        False) and allow_staging
    accesslatency = 'online'
    retentionpolicy = 'replica'
    if can_stage:
        accesslatency = 'nearline'
        retentionpolicy = 'custodial'
    saLocalID = '%s:%s:%s' % (pgroup, retentionpolicy, accesslatency)
    if can_stage:
        expirationtime = 'releaseWhenExpired'
    else:
        expirationtime = 'neverExpire'

    totalKB = sum([i.totalSpaceKB for i in pools])
    usedKB = sum([i.usedSpaceKB for i in pools])
    reservedKB = sum([i.reservedKB for i in pools])+sum([i.preciousKB for i in \
        pools])
    availableKB = sum([i.freeSpaceKB for i in pools])

    un, fn, tn = getSETape(cp, vo=pgroup)

    acbr_attr = 'GlueSAAccessControlBaseRule: %s'
    acbr = '\n'.join([acbr_attr%i for i in getAllowedVOs(cp, pgroup)])

    path = getPath(cp, pgroup)

    info = {"saLocalID"        : saLocalID,
            "seUniqueID"       : seUniqueID,
            "root"             : "/",
            "path"             : path,
            "filetype"         : "permanent",
            "saName"           : saLocalID,
            "totalOnline"      : totalKB/1024**2,
            "usedOnline"       : usedKB/1024**2,
            "freeOnline"       : availableKB/1024**2,
            "reservedOnline"   : reservedKB/1024**2,
            "totalNearline"    : tn,
            "usedNearline"     : un,
            "freeNearline"     : fn,
            "reservedNearline" : 0,
            "retention"        : retentionpolicy,
            "accessLatency"    : accesslatency,
            "expiration"       : expirationtime,
            "availableSpace"   : availableKB,
            "usedSpace"        : usedKB,
            "acbr"             : acbr,
        }

    return info

def calculate_space_from_linkgroup(cp, lg_info, pools):
    seUniqueID = cp.get("se", "unique_name")
    accesslatency = 'offline'
    if lg_info['nearline']:
        accesslatency = 'nearline'
    elif lg_info['online']:
        accesslatency = 'online'
    retentionpolicy = 'replica'
    if lg_info['custodial']:
        retentionpolicy = 'custodial'
    elif lg_info['output']:
        retentionpolicy = 'output'

    saLocalID = '%s:%s:%s' % (lg_info['name'], retentionpolicy, accesslatency)

    expirationtime = 'neverExpire'

    stateAvailable = lg_info.get('available', \
        lg_info['free']-lg_info['reserved'])/1024

    total_kb = sum([i.totalSpaceKB for i in pools])
    used_kb =  sum([i.usedSpaceKB  for i in pools])

    un, fn, tn = getSETape(cp, vo=lg_info['name'])

    acbr_attr = 'GlueSAAccessControlBaseRule: %s'
    acbr = '\n'.join([acbr_attr%i for i in getLGAllowedVOs(cp, lg_info['vos'])])

    path = getPath(cp, lg_info['name'])

    lg_info['saLocalID'] = saLocalID

    info = {"saLocalID"        : saLocalID,
            "seUniqueID"       : seUniqueID,
            "root"             : "/",
            "path"             : path,
            "filetype"         : "permanent",
            "saName"           : saLocalID,
            "totalOnline"      : total_kb/1024**2,
            "usedOnline"       : used_kb/1024**2,
            "freeOnline"       : stateAvailable/1024**2,
            "reservedOnline"   : lg_info['reserved']/1024**3,
            "totalNearline"    : tn,
            "usedNearline"     : un,
            "freeNearline"     : fn,
            "reservedNearline" : 0,
            "retention"        : retentionpolicy,
            "accessLatency"    : accesslatency,
            "expiration"       : expirationtime,
            "availableSpace"   : stateAvailable,
            "usedSpace"        : used_kb,
            "acbr"             : acbr,
        }

    return info

def calculate_voinfo_from_pgroup(cp, pgroup):
    voinfos = []
    seUniqueID = cp.get("se", "unique_name")
    for vo in getAllowedVOs(cp, pgroup):
        if vo.startswith('VO:'):
            vo = vo[3:]
        path = getPath(cp, pgroup, vo)
        id = '%s:%s:poolgroup' % (vo, pgroup)
        acbr = 'GlueVOInfoAccessControlBaseRule: %s' % vo
        info = {'voInfoID': id,
                'seUniqueID': seUniqueID,
                'name': id,
                'path': path,
                'tag': 'poolgroup',
                'acbr': acbr,
                'saLocalID': '%s:poolgroup' % pgroup
               }
        voinfos.append(info)
    return voinfos

def calculate_voinfo_from_lg(cp, lg, resv):
    """
    Calculate all the VOInfo for the LinkGroup.  Algorithm:

    0) Calculate all the allowed VOs for this link group.
    1) Calculate the allowed path for each VO/FQAN/space description.
       a) Try finding a non-default path for each space description
       b) fallback to the LinkGroup's default path.
    2) Group all the reservations by VO/FQAN, path, and space description
    3) Create one VOInfo object per FQAN/path/space description
    4) For any remaining VOs who have a path but no reserved space, create
       additional VOInfo objects.

    If the space description is "null", change the name to "DEFAULT"
    """
    acbr_spacedesc = {}
    lgId = lg['id']

    allowed_fqans = getLGAllowedVOs(cp, lg['vos'])

    # Build a list of ACBR -> unique space descriptions
    for r in resv:
        if r['linkGroupId'] != lgId:
            continue
        if 'acbr' not in r:
            r['acbr'] = getReservationACBR(cp, r['voGroup'], r['voRole'])
            if not r['acbr']:
                continue
        acbr = r['acbr']
        if acbr not in acbr_spacedesc:
            acbr_spacedesc[acbr] = sets.Set()
        spaces = acbr_spacedesc[acbr]
        spaces.add(r['descr'])

    # Rename null->DEFAULT
    for acbr, spaces in acbr_spacedesc.items():
        if 'null' in spaces:
            spaces.remove('null')
            spaces.add('DEFAULT')

    # Build a map of (acbr, path) -> unique space descriptions
    default_path = getPath(cp, lg['name'])
    acbr_path_spacedesc = {}
    for acbr, spaces in acbr_spacedesc.items():
        try:
            default_acbr_path = getPath(cp, lg['name'], acbr, \
                return_default=False)
        except:
            default_acbr_path = default_path
        for space in spaces:
            try:
                path = getPath(cp, space, acbr, return_default=False)
            except:
                path = default_acbr_path
            key = (acbr, path)
            if key not in acbr_path_spacedesc:
                acbr_path_spacedesc[key] = sets.Set()
            acbr_path_spacedesc[key].add(space)

    allowed_path = {}
    for acbr in allowed_fqans:
        try:
            default_acbr_path = getPath(cp, lg['name'], acbr, \
                return_default=False)
        except:
            default_acbr_path = default_path
        allowed_path[acbr] = default_acbr_path

    voinfos = []
    seUniqueID = cp.get("se", "unique_name")
    # Build VOInfo objects from space descriptions
    for key, spaces in acbr_path_spacedesc.items():
        acbr, path = key
        acbr = 'GlueVOInfoAccessControlBaseRule: %s' % acbr
        for space in spaces:
            id = '%s:%s' % (acbr, space)
            info = {'voInfoID': id,
                    'seUniqueID': seUniqueID,
                    'name': id,
                    'path': path,
                    'tag': space,
                    'acbr': acbr,
                    'saLocalID': lg['saLocalID']
                   }
            voinfos.append(info)
            if key[0] in allowed_fqans:
                allowed_fqans.remove(key[0])

    # Add VOInfo objects for remaining VOs
    for acbr in allowed_fqans:
        path = allowed_path[acbr]
        full_acbr = 'GlueVOInfoAccessControlBaseRule: %s' % acbr
        info = {'voInfoID': acbr,
                'seUniqueID': seUniqueID,
                'name': '%s with no reserved space' % acbr,
                'path': path,
                'tag': 'UNAVAILABLE',
                'acbr': full_acbr,
                'saLocalID': lg['saLocalID'],
               }
        voinfos.append(info)

    return voinfos

vo_re = re.compile('{(.*)}')
def getLGAllowedVOs(cp, vos):
    allowed = []
    mapper = VoMapper(cp)
    for vo_policy in vo_re.finditer(vos):
        vo_policy = vo_policy.groups()[0]
        if vo_policy == '*:*':
            return ['VO:%s' % i for i in voListStorage(cp)]
        if vo_policy.startswith('/'):
            allowed.append('VOMS:%s/Role=%s' % (vo_policy.split(':')))
        else:
            try:
                vo = mapper[vo_policy.split(':')[0]]
                allowed.append('VO:%s' % vo)
            except:
                pass
    # Remove duplicates and return
    allowed = list(sets.Set(allowed))
    return allowed

def getReservationACBR(cp, vog, vor):
    """
    Given a VO group and VO role for a space reservation, return the ACBR,
    composed of either a FQAN or other string of form ACBR_t in GLUE 1.3.

    If vog does not start with '/', then it usually means that SRM saved the
    unix username instead of the VO name; in this case, we try to use the 
    VoMapper object to determine the correct VO name.

    This will return None if the ACBR can't be determined.

    @param cp: Site config object
    @param vog: VO group string
    @param vor: VO role string
    @returns: A GLUE 1.3 complain ACBR; not necessarily a trivial one.  If no
       VO can be determined, this MAY return None.
    """
    if vog.startswith('/'):
        if vor:
            return 'VOMS:%s/Role=%s' % (vog, vor)
        else:
            return 'VOMS:%s' % vog
    else:
        mapper = VoMapper(cp)
        try:
            vo = mapper[vog]
            return 'VO:%s' % vo
        except:
            return None

def getAllowedVOs(cp, space):
    allowed_vos = cp_get(cp, "dcache", "space_%s_vos" % space, None)
    if not allowed_vos:
        allowed_vos = cp_get(cp, "dcache", "default_policy", "*")
    allowed_vos = [i.strip() for i in allowed_vos.split(',') if i.strip()]
    if '*' in allowed_vos:
        for vo in voListStorage(cp):
            if vo not in allowed_vos:
                allowed_vos.append(vo)
        allowed_vos.remove('*')
    allowed_vos = sets.Set(allowed_vos)
    return list(['VO:%s' % i for i in allowed_vos])

def getPath(cp, space, vo=None, return_default=True):
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

    If return_default is true, this is guaranteed to return a non-empty string;
    if return_default is false, then this might through a ValueError exception.

    @param cp: Site config object
    @param space: The name of the space to determine the path for.
    @param vo: The name of the VO which will be using this space; None for
        the default information.
    @kw return_default: Set to True if you want this function to return the
        default path if it cannot find a VO-specific one.
    @returns: A path string; raises a ValueError if return_default=False
    """
    default_path = cp_get(cp, "dcache", "space_%s_default_path" % space, None)
    if not default_path:
        default_path = getStoragePath(cp, vo)
    if not vo:
        return default_path
    vo_specific_paths = cp_get(cp, "dcache", "space_%s_path" % space, \
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

