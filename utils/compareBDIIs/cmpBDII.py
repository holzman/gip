#!/usr/bin/python

import os
import sys
import optparse

class BDIIError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def run_command(command):
    return os.popen(command)

def smart_bool(s):
    if s is True or s is False: return s
    s = str(s).strip().lower()
    return not s in ['false','f','n','0','']

def main():
    p = optparse.OptionParser()
    help_msg = 'Master BDII server.  Example: is.grid.iu.edu'
    p.add_option('-m', '--master-bdii', dest='master', help=help_msg, default='')
    help_msg = 'BDII server to compare.  Example: is.grid.iu.edu'
    p.add_option('-c', '--compare-bdii', dest='compare', help=help_msg, default='')
    help_msg = 'Print the differences to the screen.  Default:  False'
    p.add_option('-p', '--print-diff', dest='print_diff', help=help_msg, default=False)
    help_msg = 'Resource Group to query for.  Example: USCMS-FNAL-WC1'
    p.add_option('-r', '--resource-group', dest='resource_group', help=help_msg, default='ALL')
    help_msg = 'Save ldif output from BDIIs to a file.  Default:  False'
    p.add_option('-s', '--save-output', dest='save_output', help=help_msg, default=False)
    options, _ = p.parse_args()

    command = ""
    if options.resource_group == "ALL":
        cmd = " ldapsearch -xLLL -h %s:2170 -b mds-vo-name=local,o=grid | perl -00pe 's/\r*\n //g'"
        command = cmd % options.master
    else:
        cmd = " ldapsearch -xLLL -h %s:2170 -b mds-vo-name=%s,mds-vo-name=local,o=grid | perl -00pe 's/\r*\n //g'"
        command = cmd % (options.compare, options.resource_group)

    master_ldiff = run_command(command).readlines()
    master_ldiff = master_ldiff[4:]
    if smart_bool(options.save_output):
        fd = open("master_ldiff", 'w')
        fd.write("".join(master_ldiff))
        fd.close()

    compare_ldiff = run_command(command).readlines()
    compare_ldiff = compare_ldiff[4:]
    if options.save_output:
        fd = open("compare_ldiff", 'w')
        fd.write("".join(compare_ldiff))
        fd.close()

    master_ldiff.sort()
    for item in master_ldiff:
        item = item.lower()

    compare_ldiff.sort()
    for item in compare_ldiff:
        item = item.lower()

    ignore_list = ["GlueCEStateRunningJobs", "GlueCEStateFreeJobSlots", "GlueCEStateTotalJobs",
                   "GlueCEStateFreeCPUs", "GlueCEStateWorstResponseTime", "GlueCEStateEstimatedResponseTime",
                   "GlueCEStateWaitingJobs", "GlueSAStateUsedSpace", "GlueSAStateAvailableSpace", "GlueSESizeFree",
                   "GlueSETotalNearlineSize", "GlueSEUsedNearlineSize", "GlueSEUsedOnlineSize", "GlueSAFreeNearlineSize",
                   "GlueSAFreeOnlineSize", "GlueSAUsedOnlineSize", "GlueSATotalNearlineSize", "GlueSAUsedNearlineSize", 
                   "GlueCEInfoTotalCPUs", "GlueCEPolicyAssignedJobSlots"]

    master_ref_diffs = []
    master_is_ref_list = list(set(master_ldiff) - set(compare_ldiff))
    for item in master_is_ref_list:
        pieces = item.split(":")
        if pieces[0] in ignore_list: continue
        master_ref_diffs.append(item.strip())

    compare_ref_diffs = []
    compare_is_ref_list = list(set(compare_ldiff) - set(master_ldiff))
    for item in compare_is_ref_list:
        pieces = item.split(":")
        if pieces[0] in ignore_list: continue
        compare_ref_diffs.append(item.strip())


    if smart_bool(options.print_diff):
        print "Using the compare BDII as the reference..."
        print "=" * 80
        for item in compare_ref_diffs:
            print item.strip()
        print
        print

        print "Using the master BDII as the reference..."
        print "=" * 80
        for item in master_ref_diffs:
            print item.strip()

    return len(master_ref_diffs) + len(compare_ref_diffs)

if __name__ == "__main__":
    sys.exit(main())


