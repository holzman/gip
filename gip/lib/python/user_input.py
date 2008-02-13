
import os, sys

# GNU readline is usually only available on Linux
try:
    import readline
except:
    pass

class InputHandler:

    def __init__(self, cp):
        self.cp = cp

    def __call__(self, *args, **kw):
        return self.ask(*args, **kw)

    def ask(self, question, section, option, *verify):
        try:
            old_val = cp.get(section, option)
        except:
            old_val = None
        if question[-1] != ' ':
            question += ' '
        if old_val != None:
            question += '[%s] ' % str(old_val)
        answer = raw_input(question)
        if len(verify) > 0:
            try:
                if len(verify) > 1:
                    answer = verify[0](answer, *verify[1:])
                else:
                    answer = verify[0](answer)
            except Exception, e:
                #print e
                print "Invalid answer!  Please try again."
                return self.ask(question, section, option, *verify)
        return answer

    def pick(self, question, list):
        print question, '\n'
        counter = 0
        for entry in list:
            print '[%i] %s' % (counter, entry)
            counter += 1
        selection = None
        while selection == None:
            print "Please select a number 0-%i" % (counter-1)
            selection = raw_input()
            try:
                selection = int(selection)
            except:
                print "That is not a valid number."
                selection = None
            if selection < 0 or selection >= counter:
                print "The selection must be between 0 and %i" % (counter-1)
                selection = None
        return selection

def oneOfList(answer, list):
    if answer not in list:
        raise ValueError("Answer %s is not in list %s" % (str(answer), \
            str(list)))

def makeBoolean(answer):
    if answer.lower() == 'yes' or answer.lower() == 'y':
        return True
    if answer.lower() == 'true' or answer.lower() == 't':
        return True
    if answer.lower() == 'false' or answer.lower() == 'f':
        return False
    if answer.lower() == 'no' or answer.lower() == 'n':
        return False
    raise ValueError("Did not pass a boolean value.")

class GipQuestions:

    def __init__(self):
        old_path = list(sys.path)
        sys.path = [os.path.expandvars("$GIP_LOCATION/templates")] \
            + old_path
        questions_mod = __import__("Questions")
        sys.path = old_path
        self.__info = questions_mod

    def __getattr__(self, attr_name):
        return getattr(self.__info, attr_name)

