#!/usr/bin/env python

import time
import re
import os
import sys
import datetime
import ConfigParser

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getTemplate, getLogger
from gip_storage import execute, connect, connect_admin, voListStorage, \
    getPath, getSESpace, getSETape

log = getLogger("GIP.token_provider")

giga=1000000000L
kilo=1000L

# SQL commands to get the space token info from the DB
VOInfo_command = """
SELECT 
  vogroup,vorole,linkgroupid,description,P.name, L.name
FROM srmspace 
JOIN srmretentionpolicy P on P.id=srmspace.retentionpolicy 
JOIN srmaccesslatency L on L.id=srmspace.accesslatency
WHERE 
  state=0 AND
  description<>'null' AND
  vogroup<>'null' AND
  ( (creationtime+lifetime)>%d or lifetime<0 )
"""

SA_command = """
SELECT 
  vogroup, P.name, L.name, usedspaceinbytes, 
  sizeinbytes-usedspaceinbytes, sizeinbytes, linkgroupid, creationtime,
  lifetime
FROM srmspace
JOIN srmretentionpolicy P on P.id=srmspace.retentionpolicy 
JOIN srmaccesslatency L on L.id=srmspace.accesslatency
WHERE
  state=0 AND
  description<>'null' AND
  vogroup<>'null' AND
  ( (creationtime+lifetime)>%d or lifetime<0 )
"""

def print_VOinfo(p, cp):
    """
    Print out the VOInfo tags based upon the contents of the space reservation
    database.
    """
    command = VOInfo_command % (cp.getint('dcache_config', 'min_lifetime')*1000)
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

        path = getPath(cp, vo)
        if path == "UNDEFINED":
            continue

        info = {"voInfoID" : "%s:%s" % (cvo, description),
                "seUniqueID" : seUniqueID,
                "name" : "%s:%s" % (cvo, description),
                "path" : path,
                "tag" : description,
                "acbr" : acbr,
                "saLocalID" : "%s:%s:%s"%(cvo, retentionpolicy, accesslatency),
               }
        print VOinfo % info

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
                "usedSpace"        : available,
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

    seUniqueID = cp.get("se", "unique_name")

    try:
        stateUsed, stateAvailable = getSESpace(cp)
    except:
        stateUsed = 0
        stateAvailable = 0

    for row in rows:
        if not row[0].startswith('/'):
            row[0] = '/' + row[0]
        vo = row[0][1:].split('/')[0].lower()
        retentionpolicy = row[1].lower()
        accesslatency = row[2].lower()
        linkgroupid = long(row[6])

        # Calculate the expiration time.
        lifetime = long(row[7])
        creationtime = long(row[8])
        if lifetime >= 0:
            # I apologize for this line.  I create a datetime object from
            # the creation timestamp + lifetime timestamp, offset by the
            # timezone.  Then, I use the strftime to convert it to the
            # proper time format.
            expirationtime = (datetime.datetime.fromtimestamp((creationtime + \
                lifetime)/1000) + datetime.timedelta(0, time.timezone)). \
                strftime('%Y-%m-%dT%H:%M:%SZ')
            # Who said python can't read like perl?
        else:
            # If lifetime is negative, the space reservation won't expire
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
        if cp.has_option("tape_info", vo):
            un, fn = [i.strip() for i in cp.get("tape_info", vo).split(',')]
            tn = str(long(un) + long(fn))
        else:
            #Or, if it's not there, ignore it!
            tn = '0'
            un = '0'
            fn = '0'
        un, fn, tn = getSETape(cp, vo=vo)

        acbr = "GlueSAAccessControlBaseRule: " + vo + "\n" + \
            "GlueSAAccessControlBaseRule: VO:"+vo


        path = getPath(cp, vo)
        if path == "UNDEFINED":
            continue
        saLocalID = "%s:%s:%s" % (vo, retentionpolicy, accesslatency)
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

        print SA % info

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

