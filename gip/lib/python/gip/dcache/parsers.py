
"""
Parse the results from the SrmSpaceManager.  We do this through the admin
interface in order to avoid the setup headaches for Postgres.

Clever parsing tricks are needed because the output is currently rather crummy
compared to nice DB interactions

Example 1:
'
[dcache-head.unl.edu] (SrmSpaceManager) admin > ls
Reservations:
total number of reservations: 0
total number of bytes reserved: 0

LinkGroups:
0 Name:public-link-group FreeSpace:5152587665880 ReservedSpace:0 VOs:{*:*} onlineAllowed:true nearlineAllowed:true replicaAllowed:true onlineAllowed:true custodialAllowed:true UpdateTime:1217464991946
total number of linkGroups: 1
total number of bytes reservable: 5152587665880
last time all link groups were updated: 1217464991946
'

Example 2
'
[dcache-head.unl.edu] (SrmSpaceManager) admin > ls
Reservations:
total number of reservations: 0
total number of bytes reserved: 0

LinkGroups:
0 Name:public-link-group FreeSpace:5152587665880 ReservedSpace:0 VOs:{*:*} onlineAllowed:true nearlineAllowed:true replicaAllowed:true onlineAllowed:true custodialAllowed:true UpdateTime:1217464991946
total number of linkGroups: 1
total number of bytes reservable: 5152587665880
last time all link groups were updated: 1217464991946
[dcache-head.unl.edu] (SrmSpaceManager) admin > ls -l
Reservations:
160014 voGroup:brian voRole: linkGroupId:0 size:12345678 created:Wed Jul 30 19:46:20 CDT 2008 lifetime:86400000ms  expiration:Thu Jul 31 19:46:20 CDT 2008 descr:brian_test state:RESERVEDused:0allocated:0
160013 voGroup:brian voRole: linkGroupId:0 size:12345678 created:Wed Jul 30 19:46:05 CDT 2008 lifetime:86400000ms  expiration:Thu Jul 31 19:46:05 CDT 2008 descr:null state:RESERVEDused:0allocated:0
total number of reservations: 2
total number of bytes reserved: 24691356

LinkGroups:
0 Name:public-link-group FreeSpace:5152587665880 ReservedSpace:24691356 VOs:{*:*} onlineAllowed:true nearlineAllowed:true replicaAllowed:true onlineAllowed:true custodialAllowed:true UpdateTime:1217465172108
total number of linkGroups: 1
total number of bytes reservable: 5152587665880
last time all link groups were updated: 1217465172108
'

"""

import re
import gip_sets as sets

resv_re = re.compile('(\d+) voGroup:(.*)\s+voRole:(.*?)\s*linkGroupId:(\d+)\s*size:(\d*)\s*created:(.*?)\s*lifetime:(-*\d*)ms\s*expiration:(.*?)\s*descr:(.*?)\s*state:(.*?)\s*used:(\d+)\s*allocated:(\d+)')
lg_re = re.compile('(\d+) Name:(.*?)\s*FreeSpace:(\d+)\s*ReservedSpace:(\d+)\s*AvailableSpace:(\d+)\s*VOs:(.*?)\s*onlineAllowed:(true|false)\s*nearlineAllowed:(true|false)\s*replicaAllowed:(true|false)\s*custodialAllowed:(true|false)\s*outputAllowed:(true|false)\s*UpdateTime:(.*)\((\d+)\)')
lg2_re = re.compile('(\d+) Name:(.*?)\s*FreeSpace:(\d+)\s*ReservedSpace:(\d+)\s*VOs:(.*?)\s*onlineAllowed:(true|false)\s*nearlineAllowed:(true|false)\s*replicaAllowed:(true|false)\s*onlineAllowed:(true|false)\s*custodialAllowed:(true|false)\s*UpdateTime:(\d+)')

def parse_srm_space_manager(srm_space_manager_output):
    reservations = []
    lg = []
    for line in srm_space_manager_output.splitlines():
        m = resv_re.match(line)
        if m:
            groups = m.groups()
            info = { \
                'id': int(groups[0]),
                'voGroup': groups[1],
                'voRole': groups[2],
                'linkGroupId': int(groups[3]),
                'size': int(groups[4]),
                'created': groups[5],
                'lifetime': int(groups[6]),
                'expiration': groups[7],
                'descr': groups[8],
                'state': groups[9],
                'used': int(groups[10]),
                'allocated': int(groups[11]),
            }
            reservations.append(info)
        m = lg_re.match(line)
        if m:
            groups = m.groups()
            info = { \
                'id': int(groups[0]),
                'name': groups[1],
                'free': int(groups[2]),
                'reserved': int(groups[3]),
                'available': int(groups[4]),
                'vos': groups[5],
                'online': groups[6] == 'true',
                'nearline': groups[7] == 'true',
                'replica': groups[8] == 'true',
                'custodial': groups[9] == 'true',
                'output': groups[10] == 'true',
                'update': groups[11:13],
            }
            lg.append(info)
        m = lg2_re.match(line)
        if m:
            groups = m.groups()
            info = { \
                'id': int(groups[0]),
                'name': groups[1],
                'free': int(groups[2]),
                'reserved': int(groups[3]),
                'vos': groups[4],
                'online': groups[5] == 'true',
                'nearline': groups[6] == 'true',
                'replica': groups[7] == 'true',
                'output': groups[9] == 'true',
                'custodial': groups[8] == 'true',
                'update': int(groups[10]),
            }
            lg.append(info)
    return reservations, lg

create_pgroup = re.compile('psu create pgroup (\S+)')
create_pool = re.compile('psu create pool (\S+)')
addto_pgroup = re.compile('psu addto pgroup (\S+) (\S+)')
create_link = re.compile('psu create link (\S+)')
add_link = re.compile('psu add link (\S+) (\S+)')
create_lg = re.compile('psu create linkGroup (\S+)')
add_lg = re.compile('psu addto linkGroup (\S+) (\S+)')
linkset_re = re.compile("psu set link (\S+) -readpref=(-?[\d]+) -writepref=(-?[\d]+) -cachepref=(-?[\d]+) -p2ppref=(-?[\d]+)")

def parse_pool_manager(pool_manager_output):
    pgroups = {}
    lgroups = {}
    links = {}
    link_settings = {}
    pools = sets.Set()
    for line in pool_manager_output.splitlines():
        m = create_pool.match(line)
        if m:
            pools.add(m.groups()[0])
            continue
        m = create_pgroup.match(line)
        if m:
            pgroups[m.groups()[0]] = sets.Set()
            continue
        m = addto_pgroup.match(line)
        if m:
            group, pool = m.groups()
            pgroups[group].add(pool)
            continue
        m = create_link.match(line)
        if m:
            links[m.groups()[0]] = sets.Set()
            continue
        m = add_link.match(line)
        if m:
            link, pgroup = m.groups()
            links[link].add(pgroup)
            continue
        m = create_lg.match(line)
        if m:
            lgroups[m.groups()[0]] = sets.Set()
            continue
        m = add_lg.match(line)
        if m:
            lgroup, link = m.groups()
            lgroups[lgroup].add(link)
            continue
        m = linkset_re.match(line)
        if m:
            link, read, write, cache, p2p = m.groups()
            link_settings[link] = {'read': read, 'write': write, 'cache': cache,
                'p2p': p2p}
    return pgroups, lgroups, links, link_settings, pools

