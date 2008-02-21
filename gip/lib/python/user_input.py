
"""
A simple module for handling user input to configure the GIP.

The GIP uses simple question-and-answer based scripts to attempt to create
a valid configuration for a site.  This module takes care of:

  * Asking the user for input (even for passwords).
  * Simple validation functions for the returned input.
  * Separation of programming code and text questions for the user.
"""

import os
import sys
import socket
import getpass

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

    def password(self, *args, **kw):
        kw['password'] = True
        return self.ask(*args, **kw)

    def ask(self, question, section, option, *verify, **kw):
        password = kw.get("password", False)
        if section == None or option == None:
            old_val = None
        else:
            try:
                old_val = self.cp.get(section, option)
            except:
                #raise
                old_val = None
        if question[-1] != ' ':
            question += ' '
        if old_val != None and not password:
            question += '[%s] ' % str(old_val)
        if password:
            answer = getpass.getpass(question)
        else:
            answer = raw_input(question)
        if len(answer.strip()) == 0:
            answer = old_val
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
        if section and not self.cp.has_section(section):
            self.cp.add_section(section)
        if section and option:
            self.cp.set(section, option, str(answer))
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

def makeInt(answer):
    """
    Make sure the anwer to the question is an integer.
    """
    try:
        return int(answer)
    except:
        raise ValueError("Not a valid integer.")

def validHostname(answer):
    """
    Verify that the question's answer is a valid hostname.
    """
    try:
        socket.gethostbyname(answer)
    except:
        raise ValueError("That does not appear to be a valid hostname.")
    return answer

def validPort(answer):
    """
    Verify that the question's answer is a valid port number.
    """
    number = makeInt(answer)
    if number >= 0 and number <= 65535:
        return number
    raise ValueError("A port must be between 0 and 65535")

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

def save(cp):
    """
    Save the information in the ConfigParser to gip.conf
    """
    bkp_num = os.path.expandvars("$GIP_LOCATION/etc/gip.conf.backup.%i")
    bkp = os.path.expandvars("$GIP_LOCATION/etc/gip.conf.backup")
    save_point = os.path.expandvars("$GIP_LOCATION/etc/gip.conf")
    backup_name = None
    if os.path.exists(save_point):
        backup_name = bkp
        if os.path.exists(bkp):
            counter = 1
            while os.path.exists(bkp_num % counter):
                counter += 1
            backup_name = bkp_num % counter
        os.rename(save_point, backup_name)
    cp.write(open(save_point, 'w'))

