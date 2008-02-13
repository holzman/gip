#!/usr/bin/env python

import time, re, sys, datetime, optparse, ConfigParser
import psycopg2,psycopg2.extras


#Postgres bindings helper functions

def execute(p,command):
    """
    Given a cursor, execute a command
    """
    curs = p.cursor(cursor_factory=psycopg2.extras.DictCursor)
    curs.execute(command)
    rows = curs.fetchall()
    return rows

def connect(cp):
    """
    Connect to the SRM database based upon the parameters in the passed
    config file.
    """
    try:
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

giga=1000000000L
kilo=1000L

# The VOInfo template.  Really should be separated out into another
# file 
VOinfo=\
'''dn: GlueVOInfoLocalID=%s:%s,GlueSEUniqueID=%s,mds-vo-name=local,o=grid
objectClass: GlueSATop
objectClass: GlueVOInfo
objectClass: GlueKey
objectClass: GlueSchemaVersion
GlueVOInfoLocalID: %s:%s
GlueVOInfoName: %s:%s
GlueVOInfoPath: %s
GlueVOInfoTag: %s
%s
GlueChunkKey: GlueSALocalID=%s:%s:%s
GlueChunkKey: GlueSEUniqueID=%s
GlueSchemaVersionMajor: 1
GlueSchemaVersionMinor: 3
'''

# The SA template.  Again, should be in a separate file.
SA='''dn: GlueSALocalID=%s:%s:%s,GlueSEUniqueID=%s,mds-vo-name=local,o=grid
objectClass: GlueSATop
objectClass: GlueSA
objectClass: GlueSAPolicy
objectClass: GlueSAState
objectClass: GlueSAAccessControlBase
objectClass: GlueKey
objectClass: GlueSchemaVersion
GlueSAPath: %s
GlueSALocalID: %s:%s:%s
GlueSAPolicyFileLifeTime: permanent
GlueSAStateAvailableSpace: %s
GlueSAStateUsedSpace: %s
%s
GlueSARetentionPolicy: %s
GlueSAAccessLatency: %s
GlueSAExpirationMode: %s
GlueSATotalOnlineSize: %s
GlueSAUsedOnlineSize: %s
GlueSAFreeOnlineSize: %s
GlueSAReservedOnlineSize: %s
GlueSATotalNearlineSize: %s
GlueSAUsedNearlineSize: %s
GlueSAFreeNearlineSize: %s
GlueSAReservedNearlineSize: 0
GlueChunkKey: GlueSEUniqueID=%s
GlueSchemaVersionMajor: 1
GlueSchemaVersionMinor: 3
'''

# SQL commands I use for the above info.

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

        host = cp.get("se", "unique_name")

        if cp.has_option("vo", cvo):
            path = cp.get("vo", cvo)
        else:
            path = cp.get("vo", "default").replace("$VO", cvo)

        print VOinfo%(cvo, description, host, cvo, description, cvo,
            description, path, description, acbr, cvo, retentionpolicy,
            accesslatency, host)

def print_SA(p, cp):
    """
    Print out the SAInfo tags based upon the contents of the space reservation
    database.
    """

    command = SA_command % (cp.getint('dcache_config', 'min_lifetime')*1000)
    rows=execute(p, command)

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

        acbr = "GlueSAAccessControlBaseRule: " + vo + "\n" + \
            "GlueSAAccessControlBaseRule: VO:"+vo

        host = cp.get("se", "unique_name")

        if cp.has_option("vo", vo):
            path = cp.get("vo", vo)
        else:
            path = cp.get("vo", "default").replace("$VO", vo)

        print SA % (vo, retentionpolicy, accesslatency, host, path, vo, \
            retentionpolicy, accesslatency, str(free), str(used), acbr,
            retentionpolicy, accesslatency, expirationtime, to, uo, fo, ro,
            tn, un, fn, host)

def config_file():
    """
    Load up the config file.  It's taken from the command line, option -c
    or --config; default is gip.conf
    """
    p = optparse.OptionParser()
    p.add_option('-c', '--config', dest='config', help='Configuration file.',
        default='gip.conf')
    (options, args) = p.parse_args()
    cp = ConfigParser.ConfigParser()
    cp.read([i.strip() for i in options.config.split(',')])
    return cp

def main():
    """
    Launch the BDII info provider.
      - Load the config file
      - Connect to the Postgres DB for SRM
      - Print out the VOLocalInfo
      - Print the SALocalInfo
    """
    try:
        cp = config_file()
        p=connect(cp)
        print_VOinfo(p, cp)
        print_SA(p, cp)
        p.close()
    except:
        sys.stdout = sys.stderr
        raise

if __name__ =='__main__':
    main()

