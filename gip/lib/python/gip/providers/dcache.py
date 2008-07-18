
import sys
import services_info_provider
import token_info_provider
from generic_storage import print_classicSE
from gip_common import config, cp_getBoolean, getLogger

log = getLogger("Storage.dCache")

def main():
    """
    Print the information about a dCache storage element.

    In the case that something bad happens, we fall back to printing out the
    SRM service based on static data.
    """
    cp = config("$GIP_LOCATION/etc/dcache_storage.conf", \
            "$GIP_LOCATION/etc/dcache_password.conf", \
            "$GIP_LOCATION/etc/tape_info.conf")
    advertise_se = cp_getBoolean(cp, "se", "advertise_se", True)
    if not advertise_se:
        return
    try:
        services_info_provider.print_se(cp)
        admin = connect_admin(cp)
        services_info_provider.print_access_protocols(cp, admin)
        print_srm(cp, admin)
    except Exception, e:
        # Make sure we don't feed the error to the BDII stream;
        # fail silently, hopefully someone logs the stderr.
        services_info_provider.print_srm_compat(cp)
        sys.stdout = sys.stderr
        log.exception(e)

    try:
        # Turn SRMv2 spaces into GLUE
        p=token_info_provider.connect(cp)
        token_info_provider.print_VOinfo(p, cp)
        print_SA(p, cp)
        p.close()
    except Exception, e:
        print >> sys.stderr, e
    # Always print the fallback SA info
    token_info_provider.print_SA_compat(cp)

    # Print the ClassicSE information if necessary.
    try:
        print_classicSE(cp)
    except Exception, e:
        log.exception(e)


if __name__ == '__main__':
    main()

