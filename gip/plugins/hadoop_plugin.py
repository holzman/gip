#!/usr/bin/env python

import os
import re
import sys
import statvfs

if 'GIP_LOCATION' not in os.environ and 'OSG_LOCATION' in os.environ:
    os.environ['GIP_LOCATION'] = os.path.join(os.environ['OSG_LOCATION'],
        'gip')
if 'VDT_LOCATION' not in os.environ and 'OSG_LOCATION' in os.environ:
    os.environ['VDT_LOCATION'] = os.environ['OSG_LOCATION']
if 'JAVA_HOME' not in os.environ:
    os.environ['JAVA_HOME'] = '/usr/java/latest'
if 'HADOOP_CONF_DIR' not in os.environ:
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop'
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getLogger, cp_get, printTemplate
from gip_testing import runCommand

log = getLogger("GIP.Hadoop")

template = """\
dn: GlueSALocalID=default,GlueSEUniqueID=%(seUniqueID)s,mds-vo-name=local,o=grid
GlueSATotalOnlineSize: %(total)i
GlueSAStateUsedSpace: %(usedKB)i
GlueSAPolicyQuota: %(quotaKB)i
GlueSAUsedOnlineSize: %(used)i
GlueSAStateAvailableSpace: %(freeKB)i
GlueSAFreeOnlineSize: %(free)i
GlueSACapability: InstalledOnlineCapacity=%(total)i
"""

def getPath(cp, sect, input_path):
    #translate = cp_get(cp, sect, "mount_point", None)
    #if not translate:
    #    return input_path
    #info = translate.strip().split(",")
    #if len(info) != 2:
    #    return input_path
    #se_trim, mount = info
    #if input_path.startswith(se_trim):
    #    input_path = input_path[len(se_trim:]
    #    input_path = mount + "/" + input_path
    #    return os.path.normpath(input_path)
    #else:
    #    return input_path
    mounts = ["/mnt/hadoop", "/hadoop"]
    for mount in mounts:
        if input_path.startswith(mount):
            input_path = input_path[len(mount):]
            break
    input_path = input_path.replace("$", "")
    loc = input_path.find("VONAME")
    if loc >= 0:
        input_path = input_path[:loc]
    while input_path.endswith("/") or input_path.endswith("*"):
        input_path = input_path[:-1]
    return input_path

def rawSize(path):
    used, free, tot = 0, 0, 0
    try:
        stat_info = os.statvfs(path)
        blocks = stat_info[statvfs.F_BLOCKS]
        bsize = stat_info[statvfs.F_BSIZE]
        avail = stat_info[statvfs.F_BFREE]
    except Exception, e:
        log.exception(e)
        return used, free, tot
    used += (blocks-avail) * bsize
    free += avail          * bsize
    tot +=  blocks         * bsize
    return used, free, tot

def getUniqueID(cp, sect):
    return cp_get(cp, sect, 'unique_name', cp_get(cp, sect, 'name', 'UNKNOWN'))

path_re = re.compile("hdfs://(.*?)(/.*)")
def handle_SE(cp, sect):
    info = {}
    path = cp_get(cp, sect, "hadoop_path", cp_get(cp, sect, "default_path",
        "/"))
    hadoop_path = getPath(cp, sect, path)
    loc = path.find(hadoop_path)
    if loc >= 0:
        mount_path = path[:loc]
    else:
        if path.startswith("/mnt/hadoop"):
            mount_path = "/mnt/hadoop"
        elif path.startswith("/hadoop"):
            mount_path = "/hadoop"
        else:
            raise Exception("Unable to determine mount path from config.ini;" \
                " check section %s, attribute 'hadoop_path'" % sect)
    log.info("Detected the hadoop path of %s and mount path of %s." % \
        (hadoop_path, mount_path))
    used, free, tot = rawSize(mount_path)
    log.info("From statvfs on mount_path, used size GB: %i; free size GB: %i." \
        % (int(used/1024.**3), int(free/1024.**3)))
    cmd = "hadoop fs -count -q / %s" % hadoop_path
    fd = runCommand(cmd)
    output = fd.read()
    if fd.close():
        log.error("Non-zero output from hadoop fs -count!")
        log.error(output)
        sys.exit(1)
    log.info("Command run successfully: %s" % cmd)
    path_info = {}
    for line in output.splitlines():
        line = line.strip()
        info = line.split()
        if len(info) != 8:
            continue
        quota, remaining_quota, space_quota, remaining_space_quota, \
            dir_count, file_count, content_size, file_name = info
        m = path_re.match(file_name)
        if m:
            path = m.groups()[-1]
        else:
            continue
        try:
            space_quota = int(space_quota)
        except:
            space_quota = 0
        try:
            content_size = int(content_size)
        except:
            continue
        path_info[path] = {'space_quota': space_quota, 'content_size':
            content_size}
    if hadoop_path not in path_info:
        log.error("Hadoop path %s is not in hadoop command output." % \
            hadoop_path)
        return
    if "/" not in path_info:
        log.error("Root path / is not in hadoop command output.")
        return
    used_ratio = used / float(path_info['/']['content_size'])
    est_total = tot / used_ratio
    est_free = free / used_ratio
    est_quota = path_info[hadoop_path]['space_quota'] / used_ratio
    content_size = path_info[hadoop_path]['content_size']
    log.info("Ratio of content size (%iGB) to raw (%iGB) is %.2f" % \
        (int(content_size/1024.**3), int(used/1024.**3), used_ratio))
    info = {\
            'seUniqueID': getUniqueID(cp, sect),
            'total': int(est_total / 1024.**3),
            'usedKB': int(content_size / 1024.),
            'quotaKB': int(est_quota / 1024.),
            'used': int(content_size / 1024.**3),
            'freeKB': int(est_free / 1024.),
            'free': int(est_free / 1024.**3),
            'total': int(est_total / 1024.**3),
           }
    printTemplate(template, info)

def main():
    cp = config()
    for section in cp.sections():
        if section.lower().startswith("se_"):
            handle_SE(cp, section)

if __name__ == '__main__':
    main()

