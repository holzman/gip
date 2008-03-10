#!/usr/bin/python

import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from condor_common import parseNodes, getJobsInfo, getLrmsInfo
from gip_common import config, VoMapper, getLogger, addToPath, getTemplate, voList

log = getLogger("GIP.Condor")

def usage:
   print "Usage: osg-info-dynamic-condor.py <condor path> <ldif file> [central manager]\n"

def print_CE(cp):
	condorVersion = getLrmsInfo(cp)
	total_nodes, claimed, unclaimed = parseNodes(cp)
	vo_map = VoMapper(cp)
	jobs_info = getJobsInfo(vo_map, cp)
	ce_name = cp.get("ce", "name")
	CE_plugin = getTemplate("GlueCEPlugin", "GlueCEUniqueID")
	for vo in voList(cp):
		try:
			status = ce.get("condor", "status")
		except:
			status = "Production"
		info = jobs_info.get("vo", {"running": 0, "queued": 0, "quota": 0, "prio":0})
		info = {"version"     : condorVersion,
					"free_slots"  : unclaimed,
					"queue"       : vo,
					"job_manager" : 'condor',
					"running"     : info["running"],
					"wait"        : info["queued"],
					"total"       : total,
					"priority"    : info["prio"],
					"max_running" : info["quota"],
					"max_wall"    : 0,
					"status"      : status,
					"vo"          : vo,
					"job_slots"   : total_nodes
				}
		print CE_plugin % info
	return total_nodes, claimed, unclaimed

def print_VOViewLocal(cp):
   ce_name = cp.get("ce", "name")
   vo_map = VoMapper(cp)
   jobs_info = getJobsInfo(vo_map, cp)
   total_nodes, claimed, unclaimed = parseNodes(cp)
   VOView_plugin = getTemplate("GlueCEPlugin", "GlueVOViewLocalID")
   for vo in voList(cp):
      info = jobs_info.get("vo", {"running": 0, "queued": 0, "quota": 0, "prio":0})
      info["vo"]          = vo
      info["job_manager"] = "condor"
      info["queue"]       = vo
      info["wait"]        = info["queued"]
      info["total"]       = info["running"] + info["wait"]
      info["free_slots"]  = info["quota"]
      info["job_slots"]   = total_nodes
      print VOView_plugin % info

def main():
   try:
      cp = config()
      addToPath(cp.get("condor", "condor_path"))
      vo_map = VoMapper(cp)
      condorVersion = getLrmsInfo(cp)
      total_nodes, claimed, unclaimed = print_CE(cp)
      print_VOViewLocal(cp)
   except Exception, e:
      sys.stdout = sys.stderr
      log.error(e)
      raise

if __name__ == '__main__':
   main()

