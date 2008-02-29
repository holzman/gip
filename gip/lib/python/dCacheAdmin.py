#!/usr/bin/env python

"""
Interact with a dCache admin interface through SSH.
"""

from __future__ import generators

import signal
import sys
import os
import re
import pty
import time
import resource
import ConfigParser

ssh_extra_args = []

err_msg = """
Timeout occurred while connecting to the dCache Admin interface.  This may be
caused by an authentication error (i.e., wrong password) or may be because
the admin interface is currently down.
"""
def handler(signum, frame):
    raise Exception(err_msg)

class Admin:

  """
  Object representing the dCache Admin interface connection.

  Only one thread should access this at a time.  If you want to have multi-
  threaded access to the admin interface, the recommendation is to create one
  connection per thread.
  """

  def __init__(self, info, timeout=5):
    """
    Connects to the admin interface.

    @param info: Dictionary containing login info.  For usage in the GIP, it is
        advised to create an Admin interface by using 
        L{gip_storage.connect_admin}, not through the constructor directly.
    @param timeout: Timeout (in seconds) before the class will give up on the
        connection and raise an error.
    """
    if isinstance(info, ConfigParser.ConfigParser):
        config_file = info.get("dCacheAdmin","config_file")
        if str(config_file) == 'None':
            config_file = None
        config_section = info.get("dCacheAdmin","config_section")
        if str(config_section) == 'None':
            config_section = None
        info = parse_DBParam(config_file, config_section)
    if info.get('Interface','') != 'dCache':
      raise Exception("Must give a dCache interface!")
    if 'AdminHost' not in info:
      raise Exception("Must give an AdminHost to connect to Admin Interface.")
    self.delay = .001
    self.info = info
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout)
    self.make_connection( info )
    signal.alarm(0)
    self.location = None

  def fork_ssh( self, args ):
    self.pid, self.child = pty.fork()
    if self.pid == 0:
      str_args = ''
      for arg in args: str_args += ' ' + str(arg)
      max_fd = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
      for i in range (3, max_fd): 
        try:
          os.close (i)
        except OSError:
          pass
      try:
        os.execvp( '/usr/bin/ssh', ['ssh'] + args )
      except Exception, e:
        print e
        print "Unable to execute ssh"

  def read (self, max_read):
    time.sleep(self.delay)
    return os.read(self.child, max_read)

  def readlines( self, matches=[] ):
    time.sleep( self.delay )
    read_line = None
    re_matches = [re.compile(i) for i in matches]
    while read_line != '':
      read_line = ''; read_char = None
      stop_flag = False 
      while read_char != '' and read_char != '\n' and (not stop_flag):
        #print read_line, matches
        for regexp in re_matches:
            if regexp.search( read_line ):
                stop_flag = True
        read_char = os.read( self.child, 1 )
        read_line += read_char
      #print str(read_line), "done"
      yield str(read_line)
    return

  def write (self, text):
    time.sleep(self.delay)
    len_text = len(text) 
    assert os.write(self.child, text)  == len_text

  def make_connection(self, info):
    ssh_args = ssh_extra_args + [str(info['AdminHost'])]
    if 'Username' in info:
      ssh_args += ['-l', str(info['Username'])]
    if 'Port' in info:
      ssh_args += ['-p', str(info['Port'])]
    if 'Cipher' in info:
      ssh_args += ['-c', str(info['Cipher'])]
    if 'Protocol' in info:
      ssh_args += ['-%s' % str(info['Protocol'])]
    self.fork_ssh( ssh_args )
    #TODO: make this more robust.
    for line in self.readlines(matches=['password:','\(yes/no\)\?','>']):
      # print  "#", line.strip()
      if re.search( 'password:', line ):
        break
      if re.search( '\(yes/no\)\?', line ):
        self.write( 'yes\n' )
      if re.search( '>', line ):
        return
      if re.search( '@@@@@@@', line ):
        raise Exception("Got an error message from SSH.")
    if 'Password' in info:
      self.write( info['Password'] + '\n' )
    else:
       raise Exception("No password provided in config file, yet dCache asked for one!")
    for line in self.readlines(matches=['>']):
      #print "#", line.strip()
      if re.search( '>', line ):
        return
    raise Exception("Reached the end of the input stream, not a new prompt!")

  def logoff( self ):
    """
    Logoff the admin interface.
    """
    self.cd( None )
    self.write( 'logoff\n' )

  def cd( self, cell ):
    """
    Change to a different cell.  This will be called automatically by L{execute}
    when it is necessary.
    """
    #print self.location, str(cell)
    if self.location == str(cell):
      return
    #print "Changing directory"
    self.write('..\n')
    for line in self.readlines(matches=['>']):
      #print "#", line.strip()
      if re.search( '>', line ):
        break
    self.location = None
    if cell != None:
      cmd = 'cd ' + str(cell) + '\n'
      #print cmd
      self.write(cmd)
      for line in self.readlines(matches=['\\(%s\\) [\\w]* >' % cell,'\\([\\w]*\\) /*%s >' % cell]):
        if re.search( '>', line ):
          #print "cd:**", line
          break
      #print "cd.....", line
      self.location = str(cell)

  def execute( self, cell, command, args=[] ):
    """
    Execute a command in the admin interface

    @param cell: Cell name to send the command to.
    @param command: Command name
    @param args: Any additional arguments to pass to dCache
    @type args: List
    @returns: String containing command output.
    """
    self.cd( cell )
    if cell == None:
      cell = 'local'
    arglist = ''
    assert type(args) == type([])
    for arg in args:
      arg = str(arg)
      if arg.find('\n') >=0: raise Exception( "Newline not allowed in arguments!" )
      if not re.match('^[\-_\w]+$[\-_\w]*','-a_rg\n'): raise Exception( "Malformed argument %s" % arg )
      arglist += ' ' + str(arg)
    dc_command = str(command) + arglist
    self.write( dc_command + '\n' )
    ret_str = ''
    count = 0
    should_raise_exception = False
    no_cell_exception = False
    r1 = re.compile('java.lang.Exception')
    r2 = re.compile('No Route to cell for packet')
    r3 = re.compile('\(%s\) [\w]* >' % cell)
    r4 = re.compile('\([\w]*\) /*%s >' % cell)
    for line in self.readlines(matches=['\(%s\) [\w]* >' % cell,'\([\w]*\) /*%s >' % cell]):
      count += 1
      if r1.search( line ):
        should_raise_exception = True
      if r2.search( line ):
        no_cell_exception = True
      if r3.search( line ) or r4.search( line ):
        break 
      if count > 1:
        ret_str += line.strip() + '\n' 
    if no_cell_exception:
        raise Exception( "No route to cell %s." % cell )
    if should_raise_exception:
        raise Exception( "Timeout or other exception from cell %s." % str(self.location) )
    return ret_str

def parseOpts( args ):
  # Stupid python 2.2 on SLC3 doesn't have optparser...
  keywordOpts = {}
  passedOpts = []
  givenOpts = []
  length = len(args)
  optNum = 0
  while ( optNum < length ):
    opt = args[optNum]
    hasKeyword = False
    if len(opt) > 2 and opt[0:2] == '--':
      keyword = opt[2:]
      hasKeyword = True
    elif opt[0] == '-':
      keyword = opt[1:]
      hasKeyword = True
    if hasKeyword:
      if keyword.find('=') >= 0:
        keyword, value = keyword.split('=', 1)
        keywordOpts[keyword] = value
      elif optNum + 1 == length:
        passedOpts.append( keyword )
      elif args[optNum+1][0] == '-':
        passedOpts.append( keyword )
      else:
        keywordOpts[keyword] = args[optNum+1]
        optNum += 1
    else:
      givenOpts.append( args[optNum] )
    optNum += 1
  return keywordOpts, passedOpts, givenOpts

def find_DBParam():
    """ Search for the DBParam file in the following order:
        - $CWD/DBParam
        - $HOME/DBParam
        - /etc/DBParam
    """
    cwd = os.getcwd()
    home = os.environ.get('HOME','/etc')
    etc = '/etc'
    for location in [cwd, home, etc]:
        filename = location + '/DBParam'
        if os.path.exists( filename ):
            return filename
    raise Exception("Could not find DBParam file!")

def parse_DBParam( filename=None, section=None, interface=None ):
  if filename == None:
    filename = find_DBParam()
    #print "Using DBParam file at %s" % filename
  try:
    file = open( filename, 'r' )
  except Exception, e:
    raise Exception( "Unable to open specified DBParam file %s\nCheck the " \
                     "path and the permissions.  \nInitial exception: %s" % \
                     (filename,str(e)) )
  rlines = file.readlines()
  info = {}
  current_section = False
  for line in rlines:
    if len(line.lstrip()) == 0 or line.lstrip()[0] == '#':
      continue
    tmp = line.split(); tmp[1] = tmp[1].strip()
    if tmp[0] == "Section" and (section == None or tmp[1] == section) and \
        (interface == None or interface == tmp[1]):
      current_section = tmp[1] 
    elif tmp[0] == "Section" and tmp[1] != section:
      current_section = False
    if current_section != False:
      info[tmp[0]] = tmp[1]
  if current_section == False:
    raise Exception( "Could not find any sections in: %s\nCheck filename and "\
                     "contents!" % filename )
  return info


