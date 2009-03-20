
"""
Parser for GIP results which will help us convert test results into XML.
"""

from xml.sax.handler import ContentHandler

class GipResultsParser(ContentHandler):
    
    """
    A SAX handler which allows us to create an XML version of the GIP results
    report. 
    """
    
    def __init__(self):
        ContentHandler.__init__(self)
        self.Test = {"name" : "", "description" : "", "runtime" : "",
                     "timetaken" : "", "cases" : []}
        self.elmContents = ''

    def startDocument(self):
        self.elmContents = ''

    def findTestCase(self, sitename):
        found = False
        cases = self.Test["cases"]
        if len(cases) > 0:
            for case in cases:
                if case["site"] == sitename:
                    tc = case
                    found = True
                    break
        if not found:
            # the TestCase was not in the list
            tc = {"site" : sitename, "result" : "", "failure_count" : 0,
                  "error_count" : 0, "info_count" : 0}
            self.Test["cases"].append(tc)

        return tc

    def startElement(self, name, attrs):
        self.elmContents = ''
        if name == 'TestCase':
            attr_result = attrs.getValue("result")

            attr_name = attrs.getValue("name").split("_")
            testname = attr_name.pop(0)
            sitename = "_".join(attr_name)

            self.Test["name"] = testname

            TestCase = self.findTestCase(sitename)
            TestCase["result"] = attr_result

        elif name == 'Failure':
            attr_name = attrs.getValue("testcase").split("_")
            testname = attr_name.pop(0)
            sitename = "_".join(attr_name)

            tc = self.findTestCase(sitename)
            tc["failure_count"] += 1

        elif name == 'Error':
            attr_name = attrs.getValue("testcase").split("_")
            testname = attr_name.pop(0)
            sitename = "_".join(attr_name)

            tc = self.findTestCase(sitename)
            tc["error_count"] += 1

        elif name == 'info':
            attr_name = attrs.getValue("testcase").split("_")
            testname = attr_name.pop(0)
            sitename = "_".join(attr_name)

            tc = self.findTestCase(sitename)
            tc["info_count"] += 1

        else:
            pass

    def endElement(self, name):
        if name == 'TestRunTime':
            self.Test["runtime"] = str(self.elmContents)

        elif name == 'TestTimeTaken':
            self.Test["timetaken"] = str(self.elmContents)

        elif name == 'TestDescription':
            self.Test["description"] = str(self.elmContents)

        else:
            pass

    def characters(self, ch):
        self.elmContents += str(ch)

    def getGipResults(self):
        return self.Test
