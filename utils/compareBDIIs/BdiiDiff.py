#!/usr/bin/python

import ldap
import sys
from pprint import pprint

sdiffcount = 0

def dump_bdii(bdii):
    print "runldapquery ["+bdii+"]"

    bdiiuri = 'ldap://' + bdii + ':2170'
    l = ldap.initialize(bdiiuri)

    l.simple_bind_s('', '')

    base = "o=grid"
#    base = "mds-vo-name=USCMS-FNAL-WC1,mds-vo-name=local,o=grid"
    scope = ldap.SCOPE_SUBTREE
    timeout = 0
    result_set = []

    filter = '(!(|(GlueLocationLocalID=GIP_VERSION)' + \
             '(GlueLocationLocalID=VDT_VERSION)' + \
             '(objectClass=Mds)' + \
             '(GlueLocationLocalID=TIMESTAMP)))' 

    try:
        result_id = l.search(base, scope, filter)
        while 1:
            result_type, result_data = l.result(result_id, timeout)
            if (result_data == []):
                break
            else:
                if result_type == ldap.RES_SEARCH_ENTRY:
                    result_set.append(result_data)

    except ldap.LDAPError, error_message:
        print error_message

    return result_set

bdii1 = 'is1.grid.iu.edu'
bdii2 = 'is-itb2.grid.iu.edu'

is1 = dump_bdii(bdii1)
is2 = dump_bdii(bdii2)

ignore_attrs = ['GlueCEStateFreeJobSlots',
                'GlueCEStateRunningJobs',
                'GlueCEStateTotalJobs',
                'GlueCEStateWaitingJobs',
                'GlueCEStateEstimatedResponseTime',
                'GlueCEStateWorstResponseTime',
                'GlueCEStateFreeCPUs',
                'GlueCEInfoTotalCPUs',
                'GlueCEPolicyAssignedJobSlots',
                'GlueSAStateAvailableSpace',
                'GlueSAStateUsedSpace',
                'GlueSAUsedOnlineSize',
                'GlueSAFreeOnlineSize',
                'GlueSESizeFree',
                'GlueSEUsedOnlineSize',
                'GlueSATotalOnlizeSize',
                'GlueSACapability',
                'GlueSESizeTotal',
                'GlueSATotalOnlineSize',
                'GlueSAReservedOnlineSize',
                'GlueSETotalOnlineSize',
                'GlueSiteSponsor',
                'GlueCEPolicyMaxRunningJobs'
                ]

def print_stanza_diffs(dn, stanza1, stanza2):
    global sdiffcount
    for k in stanza2.keys():
        if not stanza1.has_key(k):
            print "Missing key [#1]: dn: %s\n\t[%s]" % (caseDn[dn], k)
            
    for k in stanza1.keys():
        if not stanza2.has_key(k):
            print "Missing key [#2]: dn: %s\n\t[%s]" % (caseDn[dn], k)
            continue
        
        if k in ignore_attrs: continue
        sdiff = set(stanza1[k]).symmetric_difference(set(stanza2[k]))
        if sdiff:
            sdiffcount += 1
            print "Difference: %s\n\t[%s]: %s" % (caseDn[dn], k, list(sdiff))

    return

def get_data(item):
    dn = item[0][0]
    stanza = item[0][1]
    return dn, stanza

print 'BDII #1: %s\tBDII#2: %s\n' % (bdii1, bdii2)

is1dict = {}
is2dict = {}
caseDn = {}

for item in is1:
     dn, stanza = get_data(item)
     caseDn[dn.lower()] = dn
     is1dict[dn.lower()] = item[0][1]

for item in is2:
     dn, stanza = get_data(item)
     caseDn[dn.lower()] = dn
     is2dict[dn.lower()] = item[0][1]

if is1dict.has_key('o=grid'):
    del is1dict['o=grid']
    
for dn in is1dict.keys():
    matched = False
#    print "Testing dn: %s" % caseDn[dn]
    if dn in is2dict:
        matched = True
        if is1dict[dn] != is2dict[dn]:
            print_stanza_diffs(dn, is1dict[dn], is2dict[dn])
        
    if matched == False:
        print 'Missing DN [#2]: %s' % caseDn[dn]
        sdiffcount +=1

if sdiffcount > 255: sdiffcount = 255
sys.exit(sdiffcount)

