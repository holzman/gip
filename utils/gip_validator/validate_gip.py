#!/usr/bin/env python

import os
import sys
import imp
import types

from lib.validator_config import config, cp_get, cp_getBoolean, cp_getList
from lib.validator_common import MSG_CRITICAL, compare_by, ls, write_file
from lib.validator_common import getTimestamp
from lib.validator_base import Base
from lib.xml_common import XMLDom

EXIT_OK = 0
EXIT_FAIL = 101

def load_base_modules(pkg):
    path = ""
    package_list = pkg.split(".")
    for p in package_list:
        if path == "":
            fp, pathname, description = imp.find_module(p)
        else:
            fp, pathname, description = imp.find_module(p, path)
        mod = imp.load_module(p, fp, pathname, description)
        path = mod.__path__
    return mod

def discoverTests(package, cp):
    test_list = []
    module_list = []
    test_dir = os.path.expandvars("$VALIDATOR_LOCATION/%s" % str(package).replace(".", "/"))
    dir_listing = ls(test_dir)
    for dir_item in dir_listing:
        if dir_item.endswith(".py") and not dir_item.startswith("__init__"):
            module_list.append(dir_item)

    for mod in module_list:
        module = load_base_modules(package)
        fp, pathname, description = imp.find_module(mod.rstrip(".py"), module.__path__)
        module = imp.load_module(mod.rstrip(".py"), fp, pathname, description)
        attrs = dir(module)
        for name in attrs:
            obj = getattr(module, name)
            if isinstance(obj, (type, types.ClassType)):
                if issubclass(obj, Base) and not obj is Base:
                    test_list.append(obj(cp))

    return test_list
    
def getTests(cp):
    test_list = []
    
    # which tests should be enabled?
    osg_enabled = cp_getBoolean(cp, "tests", "osg_enabled", True)
    glite_enabled = cp_getBoolean(cp, "tests", "glite_enabled", False)
    reports_enabled = cp_getBoolean(cp, "tests", "reports_enabled", False)
    utility_enabled = cp_getBoolean(cp, "tests", "utility_enabled", False)
    
    if osg_enabled:
        package = "lib.tests.osg"
        test_list.extend(discoverTests(package, cp))

    if glite_enabled:
        package = "lib.tests.glite"
        test_list.extend(discoverTests(package, cp))
        
    if reports_enabled:
        package = "lib.tests.reports"
        test_list.extend(discoverTests(package, cp))
        
    if utility_enabled:
        package = "lib.tests.utility"
        test_list.extend(discoverTests(package, cp))
        
    return test_list

def runTests(test_list, site_list):
    results_list = []
    for test in test_list:
        results_list.extend(test.run(site_list))

    return results_list

def determineExitCode(results_list):
    # TODO: make the test type dynamic
    # Note, just initializing the OSG test, the rest can be set dynamically
    test_result = {"OSG" : EXIT_OK}
    for result in results_list:
        if result["result"] == MSG_CRITICAL:
            test_result[str(result["type"]).upper()] = EXIT_FAIL

    exit_code = EXIT_OK
    for key in test_result.keys():
        if test_result[key] > EXIT_OK: 
            exit_code += test_result[key]

    return exit_code

def addDocument(dom):
    root = dom.addChild(dom.doc, "gip_validator")
    root.setAttribute("run_time", getTimestamp())
    return root

def siteTemplate(dom, root_leaf, site_name):
    site_leaf = dom.addChild(root_leaf, "site")
    site_leaf.setAttribute("name", site_name)
    site_leaf.setAttribute("overall_status", "OK")
    
    osg_leaf = dom.addChild(site_leaf, "osg_test")
    osg_leaf.setAttribute("overall_status", "NA")
    
    glite_leaf = dom.addChild(site_leaf, "glite_test")
    glite_leaf.setAttribute("overall_status", "NA")
    
    reports_leaf = dom.addChild(site_leaf, "reports_test")
    reports_leaf.setAttribute("overall_status", "NA")
    
    utility_leaf = dom.addChild(site_leaf, "utility_test")
    utility_leaf.setAttribute("overall_status", "NA")
    
    return site_leaf, osg_leaf, glite_leaf, reports_leaf, utility_leaf

def addTest(dom, leaf, test_entry):
    test_leaf = dom.addChild(leaf, "test")
    test_leaf.setAttribute("name", test_entry["name"])
    if test_entry["result"] == "PASS":
        test_leaf.setAttribute("status", "OK")
    else:
        test_leaf.setAttribute("status", "FAIL")
    for msg in test_entry["messages"]:
        test_msg = dom.addChild(test_leaf, "test_msg", msg["msg"])
        test_msg.setAttribute("status", msg["type"])
    return test_entry["result"]

def writeResults(results_list, path):
    xml = XMLDom()
    root = addDocument(xml)
    results_list.sort(compare_by("site"))
    if len(results_list) > 0:
        site = results_list[0]["site"]
        site_leaf, osg_leaf, glite_leaf, reports_leaf, utility_leaf = siteTemplate(xml, root, site)
        for result in results_list:
            if not result["site"] == site:
                site = result["site"]
                site_leaf, osg_leaf, glite_leaf, reports_leaf, utility_leaf = siteTemplate(xml, root, site)
            if str(result["type"]).upper() == "OSG":
                test_result = addTest(xml, osg_leaf, result)
            if str(result["type"]).upper() == "GLITE":
                test_result = addTest(xml, glite_leaf, result)
            if str(result["type"]).upper() == "REPORTS":
                test_result = addTest(xml, reports_leaf, result)
            if str(result["type"]).upper() == "UTILITY":
                test_result = addTest(xml, utility_leaf, result)
            if test_result == MSG_CRITICAL:
                site_leaf.setAttribute("overall_status", "FAIL")
        xml_str = xml.toXML(pretty=True)
    else:
        xml_str = "<xml></xml>"
    
    write_file(path, xml_str)
    
def main(args):
    os.environ["VALIDATOR_LOCATION"] = os.path.abspath(sys.path[0])
                    
    cp = config(args)
    write_results = cp_getBoolean(cp, "validator", "write_results")
    # get site list
    site_list = cp_getList(cp, "validator", "site_names", [])
    test_list = getTests(cp)

    results_list = runTests(test_list, site_list)
    exit_code = determineExitCode(results_list)
    if write_results:
        results_path = cp_get(cp, "validator", "results_path")
        writeResults(results_list, results_path)
    return exit_code
         
if __name__ == '__main__':
    sys.exit(main(sys.argv))
