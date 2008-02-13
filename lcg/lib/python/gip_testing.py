
import os

replace_command = False

commands = {}

def lookupCommand(cmd):
    cmd = cmd.strip()
    if cmd not in commands:
        fd = open(os.path.expandvars("$VDT_LOCATION/test/command_output/" \
            "commands"))
        for line in fd:
            if line.startswith("#") or len(line.strip()) == 0:
                continue
            command, val = line.split(':', 1)
            val = val.strip()
            commands[val.strip()] = command.strip()
    return commands[cmd]

def runCommand(cmd, force_command=False):
    if replace_command and not force_command:
        try:
            filename = lookupCommand(cmd)
        except Exception, e:
            print e
            return runCommand(cmd, force_command=True)
        return open(os.path.expandvars("$VDT_LOCATION/test/command_output/%s" \
            % filename))
    else:
        return os.popen(cmd)

