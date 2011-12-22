#!/usr/bin/env python

import os
import sys

if 'GIP_LOCATION' in os.environ:
    sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getLogger, cp_get, cp_getBoolean
from gip.providers.pbs import main as pbs_main
from gip.providers.condor import main as condor_main
from gip.providers.sge import main as sge_main
from gip.providers.lsf import main as lsf_main
from gip.providers.slurm import main as slurm_main

log = getLogger("GIP.BatchSystem")

def main():
    cp = config()
    se_only = cp_getBoolean(cp, "gip", "se_only", False)
    if not se_only:
        job_manager = cp_get(cp, "ce", "job_manager", None)
        if job_manager:
            log.info("Using job manager %s" % job_manager)
        else:
           log.error("Job manager not specified!")
           sys.exit(2)
        if job_manager == 'pbs':
            pbs_main()
        elif job_manager == 'condor':
            condor_main()
        elif job_manager == 'sge':
            sge_main()
        elif job_manager == 'lsf':
            lsf_main()
        elif job_manager == 'slurm':
            slurm_main()
        else:
            log.error("Unknown job manager: %s." % job_manager)
            sys.exit(1)

if __name__ == '__main__':
    main()

