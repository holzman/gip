
import gip_common
import gip_ldap

def compare_CE(cp):
    entries = gip_ldap.read_bdii(cp, query="(objectClass=GlueCE)")
    print "CE Comparison info:"
    tot = 0
    tot2 = 0
    idx = 0
    idx2 = 0
    for entry in entries:
        ceName = entry.glue['CEUniqueID']
        ert, wrt = gip_common.responseTimes(cp,
            entry.glue['CEStateRunningJobs'], entry.glue['CEStateWaitingJobs'])
        try:
            actual_ert = int(entry.glue['CEStateEstimatedResponseTime'])
            actual_wrt = int(entry.glue['CEStateWorstResponseTime'])
            running = int(entry.glue['CEStateRunningJobs'])
            waiting = int(entry.glue['CEStateWaitingJobs'])
        except:
            continue
        if actual_ert == 0 and waiting > 0:
            print "Bad ERT: CE %s, ERT=0, %i waiting." % (ceName, waiting)
        if running + waiting <= 100: # Ignore the small case
            continue
        if actual_ert > 1e6: # Nasty huge values
            continue
        if str(actual_ert)[-3:] == '000': # Suspicious - hardcoded?
            continue
        if str(actual_ert) == '777777': # Apparently hardcoded in Italy...
            continue
        if ceName.find('.edu') >= 0 or ceName.find('.br') >= 0: # Ignore OSG GIP sites
            continue
        if actual_ert == 0 and waiting > 10:
            continue
        idx += 1
        print '\t- %s' % ceName
        print "\t\t* Running: %4s, Waiting: %4s" % \
            (entry.glue['CEStateRunningJobs'], entry.glue['CEStateWaitingJobs'])
        diff = abs(ert-actual_ert)/float(3600.)
        if actual_wrt == 0:
            diff2 = 0
        else:
            diff2 = (wrt - actual_wrt)/float(actual_wrt)
        tot += diff
        hitMax = ert >= 86400 or actual_ert >= 86400
        if hitMax == False:
            tot2 += diff
            idx2 += 1
        print '\t\t* Computed ERT: %6i, Actual ERT: %6i, Difference In Hours %.1f' % \
            (ert, actual_ert, diff)
        #print '\t\t* Computed WRT: %6i, Actual WRT: %6i, Difference %%: %.2f' % \
        #    (wrt, actual_wrt, diff2)
    print "Average difference in hours: %.2f" % (tot/float(idx))
    print "Average difference in hours, for sites which didn't hit max ERT: %.2f" % (tot2/float(idx2))

def compare_VOInfo(cp):
    raise NotImplementedError()

def main():
    cp = gip_common.config()
    compare_CE(cp)
    compare_VOInfo(cp)

if __name__ == '__main__':
    main()

