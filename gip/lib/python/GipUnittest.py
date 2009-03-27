#!/usr/bin/env python

import time
import sys
import os

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
import unittest
from gip_common import cp_getBoolean

class GipTestCase(unittest.TestCase):
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)
        self.methodName = methodName
        self.name = ""
        self.cp = None
        self.result_ref = None
        self.override = False

    def setCp(self, cp):
        self.cp = cp
        self.override = cp_getBoolean(self.cp, "gip_tests", "use_xml")

    def run(self, result=None):
        if result is None: result = GipTextTestResult()
        self.result_ref = result
        unittest.TestCase.run(self, result)

    def __call__(self, result=None):
        if result is None: result = GipTextTestResult()
        self.result_ref = result
        unittest.TestCase.__call__(self, result)

    def shortDescription(self):
        if self.override:
            testMethod = getattr(self, self.methodName)
            return testMethod.__doc__
        else:
            return self.getname()

    def getname(self):
        return self.name

    def expectEquals(self, first, second, msg=None, critical=False):
        if not first == second:
            self.result_ref.addInfo(self, msg, critical)

    def expectNotEquals(self, first, second, msg=None, critical=False):
        if first == second:
            self.result_ref.addInfo(self, msg, critical)

    def expectAlmostEquals(self, first, second, places=7, msg=None, critical=False):
        if round(second-first, places) != 0:
            self.result_ref.addInfo(self, msg, critical)

    def expectNotAlmostEquals(self, first, second, places=7, msg=None, critical=False):
        if round(second-first, places) == 0:
            self.result_ref.addInfo(self, msg, critical)

    def expectTrue(self, expr, msg=None, critical=False):
        if not expr:
            self.result_ref.addInfo(self, msg, critical)

    expectEqual = expectEquals

    expectNotEqual = expectNotEquals

    expectAlmostEqual = expectAlmostEquals

    expectNotAlmostEqual = expectNotAlmostEquals

class GipXmlTestRunner:

    def __init__(self, stream=sys.stdout):
        self.stream = stream

    def writeUpdate(self, message):
        if message:
            self.stream.write(message)

    def run(self, test):
        updateDateTime = time.strftime("%A %b %d %Y %H:%M:%S")
        # Define the test results class
        result = GipXmlTestResult(self)

        # write out xml headers
        self.writeUpdate('<?xml version="1.0" encoding="UTF-8"?>\n')
        self.writeUpdate('<?xml-stylesheet type="text/xsl" href="gip_test.xsl"?>\n')

        # Start the xml document
        self.writeUpdate("<TestRun>\n")

        self.writeUpdate("<TestRunTime>\n")
        self.writeUpdate('<' + '![CDATA[' + updateDateTime + ']]' + '>\n')
        self.writeUpdate("</TestRunTime>\n")
        # Write out the description for the test
        self.writeUpdate("<TestDescription>\n")
        self.writeUpdate('<' + '![CDATA[')
        short_description = test._tests[0].shortDescription()
        if not short_description:
            short_description = "(No description available)"
        self.writeUpdate(test._tests[0].shortDescription())
        self.writeUpdate(']]' + '>\n')
        self.writeUpdate("</TestDescription>\n")

        # Setup timing to determine how long the tests run
        startTime = time.time()
        # run the tests
        test(result)
        stopTime = time.time()
        # calculate the time it took to run the tests
        timeTaken = float(stopTime - startTime)

        # record the time taken
        self.writeUpdate("<TestTimeTaken>")
        self.writeUpdate(str(timeTaken))
        self.writeUpdate("</TestTimeTaken>\n")

        # write out any test errors or test failures
        result.printErrors()
        result.printInfo()
        run = result.testsRun

        # write out the document closing tag
        self.writeUpdate("</TestRun>\n")

        return result

class GipXmlTestResult(unittest.TestResult):
    """
    A test result class that can print GipXmlTestRunner
    """
    def __init__(self, runner):
        unittest.TestResult.__init__(self)
        self.runner = runner
        self.info = []

    def startTest(self, test):
        unittest.TestResult.startTest(self, test)
        # TODO: we should escape quotes here
        try:
            name = test.getname()
        except:
            name = "Unknown"
        self.runner.writeUpdate('<TestCase name="%s" ' % name)

    def addSuccess(self, test):
        unittest.TestResult.addSuccess(self, test)
        self.runner.writeUpdate('result="ok" />\n')

    def addError(self, test, err):
        unittest.TestResult.addError(self, test, err)
        self.runner.writeUpdate('result="error" />\n')

    def addFailure(self, test, err):
        unittest.TestResult.addFailure(self, test, err)
        self.runner.writeUpdate('result="fail" />\n')

    def addInfo(self, test, info_string, critical):
        self.info.append((test, info_string, critical))

    def printErrors(self):
        self.printErrorList('Error', self.errors)
        self.printErrorList('Failure', self.failures)

    def printErrorList(self, flavor, errors):
        count = 0
        for test, err in errors:
            self.runner.writeUpdate('<%s testcase="%s">\n' % (flavor, test.getname()))
            self.runner.writeUpdate('<' + '![CDATA[')
            self.runner.writeUpdate("%s" % err)
            self.runner.writeUpdate(']]' + '>\n')
            self.runner.writeUpdate("</%s>\n" % flavor)
            count += 1

        self.runner.writeUpdate('<%s_count count="%i"/>\n' % (flavor, count))

    def _exc_info_to_string(self, err, test=None):
        """Converts a sys.exc_info()-style tuple of values into a string."""
        exctype, value, tb = err
        return ''.join(str(value))

    def printInfo(self):
        count = 0
        for test, info_string, critical in self.info:
            self.runner.writeUpdate('<info testcase="%s" critical="%s">\n' % (test.getname(), critical))
            self.runner.writeUpdate('<' + '![CDATA[')
            self.runner.writeUpdate("%s" % info_string)
            self.runner.writeUpdate(']]' + '>\n')
            self.runner.writeUpdate("</info>\n")
            count += 1

        self.runner.writeUpdate('<info_count count="%i"/>\n' % count)

class GipTextTestResult(unittest._TextTestResult):
    def __init__(self, stream, descriptions, verbosity):
        unittest._TextTestResult.__init__(self, stream, descriptions, verbosity)
        self.info = []

    def addInfo(self, test, info_string, critical):
        self.info.append((test, info_string, critical))
        if self.showAll:
            self.stream.writeln("INFO")
        elif self.dots:
            self.stream.write('I')

    def printInfo(self):
        if self.dots or self.showAll:
            self.stream.writeln()
        for test, info_string, critical in self.info:
            self.stream.writeln(self.separator1)
            self.stream.writeln("%s: %s" % ('INFO',self.getDescription(test)))
            self.stream.writeln(self.separator2)
            self.stream.writeln("%s" % info_string)

class GipTextTestRunner(unittest.TextTestRunner):
    def __init__(self, stream=sys.stderr, descriptions=1, verbosity=1):
        unittest.TextTestRunner.__init__(self, stream, descriptions, verbosity)

    def _makeResult(self):
        return GipTextTestResult(self.stream, self.descriptions, self.verbosity)

    def run(self, test):
        result = unittest.TextTestRunner.run(self, test)
        result.printInfo()
        return result
