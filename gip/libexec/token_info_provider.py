#!/usr/bin/env python

import time
import re
import os
import sys
import datetime
import ConfigParser

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getTemplate, getLogger, cp_get
from gip_storage import execute, connect, connect_admin, voListStorage, \
    getPath, getSESpace, getSETape

log = getLogger("GIP.token_provider")

giga=1000000000L
kilo=1000L

# SQL commands to get the space token info from the DB
VOInfo_command = """
SELECT 
  vogroup,vorole,linkgroupid,description,P.name, L.name,LG.name
FROM srmspace 
JOIN srmretentionpolicy P on P.id=srmspace.retentionpolicy 
JOIN srmaccesslatency L on L.id=srmspace.accesslatency
JOIN srmlinkgroup LG on LG.id=srmspace.linkgroupid
WHERE 
  state=0 AND
  description<>'null' AND
  vogroup<>'null' AND
  ( (creationtime+lifetime)>%d or lifetime<0 )
"""

SA_command = """
SELECT 
  LG.name, P.name, L.name, sum(usedspaceinbytes), 
  sum(sizeinbytes-usedspaceinbytes), sum(sizeinbytes), linkgroupid, 
  min(creationtime), max(lifetime)
FROM srmspace
JOIN srmretentionpolicy P on P.id=srmspace.retentionpolicy 
JOIN srmaccesslatency L on L.id=srmspace.accesslatency
JOIN srmlinkgroup LG on LG.id=srmspace.linkgroupid
WHERE
  state=0 AND
  description<>'null' AND
  vogroup<>'null' AND
  ( (creationtime+lifetime)>%d or lifetime<0 )
GROUP BY
  LG.name, P.name, L.name, retentionpolicy, accesslatency, linkgroupid
"""

SA_VOs_command = """
SELECT 
  vogroup, vorole 
FROM srmlinkgroupvos
WHERE
  linkgroupid=%i
"""

def print_VOinfo(p, cp):
    """
    Print out the VOInfo tags based upon the contents of the space reservation
    database.
    """
    min_lifetime = int(cp_get(cp, 'dcache_config', 'min_lifetime', 60))
    command = VOInfo_command % (min_lifetime*1000)
    rows=execute(p,command)

    seUniqueID = cp.get("se", "unique_name")
    VOInfo = getTemplate("GlueSE", "GlueVOInfoLocalID")

    for row in rows:
        if not row[0].startswith('/'):
            row[0] = '/' + row[0]
        cvo=row[0][1:].split('/')[0].lower()
        if len(row[0].split('/')) > 2:
            vog = row[0]
        else:
            vog = None 
        if row[1] != None and len(row[1].strip()) > 0:
            vor=row[1].split('/')[0].lower()
        else:
            vor = None
        linkgroupid=row[2]
        description=row[3]
        retentionpolicy=row[4].lower()
        accesslatency=row[5].lower()
        link_group=row[6].lower()

        # Determine the access control based upon whether this is a VO,
        # VO group, or VO group plus VO role.
        acbr=''
        if vog != None and vor == None: 
            acbr = "GlueVOInfoAccessControlBaseRule: VOMS:"+vog
        elif vog != None and vor != None:
            acbr = "GlueVOInfoAccessControlBaseRule: VOMS:"+vog+"/Role="+vor
        elif cvo != None and vor != None:
            acbr = "GlueVOInfoAccessControlBaseRule: VOMS:/"+cvo+"/Role="+vor
        elif vor == None:
            acbr = "GlueVOInfoAccessControlBaseRule: VO:" + cvo 
        else:
            continue

        path = getPath(cp, cvo)
        if path == "UNDEFINED":
            continue

        info = {"voInfoID" : "%s:%s" % (cvo, description),
                "seUniqueID" : seUniqueID,
                "name" : "%s:%s" % (cvo, description),
                "path" : path,
                "tag" : description,
                "acbr" : acbr,
                "saLocalID" : "%s:%s:%s"%(link_group, retentionpolicy, accesslatency),
               }
        print VOInfo % info

def print_SA_compat(cp):
    """
    Print out the SALocal information for backward compatibility with 
    GLUE 1.2
    """
    vos = voListStorage(cp)
    se_unique_id = cp.get("se", "unique_name")
    se_name = cp.get("se", "name")
    saTemplate = getTemplate("GlueSE", "GlueSALocalID")
    try:
        used, available = getSESpace(cp)
    except Exception, e:
        #raise
        log.error("Unable to get SE space: %s" % str(e))
        used = 0
        available = 0
    for vo in vos:
        acbr = "GlueSAAccessControlBaseRule: %s\n" % vo
        acbr += "GlueSAAccessControlBaseRule: VO:%s" % vo
        info = {"saLocalID"        : vo,
                "seUniqueID"       : se_unique_id,
                "root"             : "/",
                "path"             : getPath(cp, vo),
                "filetype"         : "permanent",
                "saName"           : "%s_default" % vo,
                "totalOnline"      : 0,
                "usedOnline"       : 0,
                "freeOnline"       : 0,
                "reservedOnline"   : 0,
                "totalNearline"    : 0,
                "usedNearline"     : 0,
                "freeNearline"     : 0,
                "reservedNearline" : 0,
                "retention"        : "replica",
                "accessLatency"    : "online",
                "expiration"       : "neverExpire",
                "availableSpace"   : available,
                "usedSpace"        : used,
                "acbr"             : acbr,
               }
        print saTemplate % info

def print_SA(p, cp):
    """
    Print out the SAInfo tags based upon the contents of the space reservation
    database.
    """

    command = SA_command % (cp.getint('dcache_config', 'min_lifetime')*1000)
    rows=execute(p, command)
    saTemplate = getTemplate("GlueSE", "GlueSALocalID")

    seUniqueID = cp.get("se", "unique_name")

    try:
        stateUsed, stateAvailable = getSESpace(cp)
    except:
        stateUsed = 0
        stateAvailable = 0

    for row in rows:
        #if not row[0].startswith('/'):
        #    row[0] = '/' + row[0]
        #vo = row[0][1:].split('/')[0].lower()
        link_group = row[0]
        retentionpolicy = row[1].lower()
        accesslatency = row[2].lower()
        linkgroupid = long(row[6])

        # Calculate the expiration time.
        #lifetime = long(row[7])
        #creationtime = long(row[8])
        #if lifetime >= 0:
        #    # I apologize for this line.  I create a datetime object from
        #    # the creation timestamp + lifetime timestamp, offset by the
        #    # timezone.  Then, I use the strftime to convert it to the
        #    # proper time format.
        #    expirationtime = (datetime.datetime.fromtimestamp((creationtime + \
        #        lifetime)/1000) + datetime.timedelta(0, time.timezone)). \
        #        strftime('%Y-%m-%dT%H:%M:%SZ')
        #    # Who said python can't read like perl?
        #else:
        #    # If lifetime is negative, the space reservation won't expire
        #    expirationtime = 'neverExpire'
        expirationtime = 'neverExpire'

        used = str(long(row[3])/kilo)
        free = str(long(row[4])/kilo)

        # Use srmlinkgroup table to tell us the reserved and freespace for 
        # this link-group
        command = """
            SELECT
                freespaceinbytes,
                reservedspaceinbytes
            FROM srmlinkgroup
            WHERE id=%i
        """
        rows2 = execute(p, command % linkgroupid)
        for row2 in rows2:
            freespace = long(row2[0])
            fo = str(freespace/giga)
            ro = str(long(row2[1])/giga)

        # Add up all usage for this linkgroup to get used online;
        # freespace + usedonline = totalonline, I guess
        command = """
            SELECT
                sum(usedspaceinbytes)
            FROM srmspace
            WHERE linkgroupid=%i AND
            (lifetime < 0 OR
            date_part('epoch', now()) < (creationtime+lifetime)/1000)
        """
        rows2 = execute(p, command % linkgroupid)
        for row2 in rows2:
            usedspace = long(row2[0])
            uo = long(usedspace/giga)
            to = str((freespace+usedspace)/giga)

        # Load up the tape statistics from the config file
        #if cp.has_option("tape_info", link_group):
        #    un, fn = [i.strip() for i in cp.get("tape_info", link_group).\
        #              split(',')]
        #    tn = str(long(un) + long(fn))
        #else:
        #    #Or, if it's not there, ignore it!
        #    tn = '0'
        #    un = '0'
        #    fn = '0'
        un, fn, tn = getSETape(cp, vo=link_group)


        # Do the hard work of determining the acbr:
        acbr = ''
        rows3 = execute(p, SA_VOs_command % linkgroupid)
        access_perms = []
        for acbrrow in rows3:
            vog, vor = acbrrow
            if vog == '*':
                vos = voListStorage(cp)
                for vo in vos:
                    vo_entry = "VO:%s" % vo
                    if vo not in access_perms:
                        access_perms.append(vo)
                    if vo_entry not in access_perms:
                        access_perms.append(vo_entry)
            elif vor == '*':
                vog_entry = "VOMS:%s" % vog
                if vog_entry not in access_perms:
                    access_perms.append(vog_entry)
            else:
                voms_entry = "VOMS:%s/Role=%s" % (vog, vor)
                if voms_entry not in access_perms:
                    access_perms.append(voms_entry)
        for entry in access_perms:
            acbr += "GlueSAAccessControlBaseRule: %s\n" % entry
        acbr = acbr[:-1]

        path = getPath(cp, '')
        if path == "UNDEFINED":
            continue
        saLocalID = "%s:%s:%s" % (link_group, retentionpolicy, accesslatency)
        info = {"saLocalID"        : saLocalID,
                "seUniqueID"       : seUniqueID,
                "root"             : "/",
                "path"             : path,
                "filetype"         : "permanent",
                "saName"           : saLocalID,
                "totalOnline"      : to,
                "usedOnline"       : uo,
                "freeOnline"       : fo,
                "reservedOnline"   : ro,
                "totalNearline"    : tn,
                "usedNearline"     : un,
                "freeNearline"     : fn,
                "reservedNearline" : 0,
                "retention"        : retentionpolicy,
                "accessLatency"    : accesslatency,
                "expiration"       : expirationtime,
                "availableSpace"   : stateAvailable,
                "usedSpace"        : stateUsed,
                "acbr"             : acbr,
               }

        print saTemplate % info

def main():
    """
    Launch the BDII info provider.
      - Load the config file
      - Connect to the Postgres DB for SRM
      - Print out the VOLocalInfo
      - Print the SALocalInfo
    """
    try:
        cp = config("$GIP_LOCATION/etc/dcache_storage.conf", \
                    "$GIP_LOCATION/etc/dcache_password.conf", \
                    "$GIP_LOCATION/etc/tape_info.conf")
        try:
            p=connect(cp)
            print_VOinfo(p, cp)
            print_SA(p, cp)
            p.close()
        except Exception, e:
            print >> sys.stderr, e
        print_SA_compat(cp)
    except:
        sys.stdout = sys.stderr
        raise

if __name__ =='__main__':
    main()

