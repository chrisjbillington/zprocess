import zlock

try:
    import ConfigParser
    from LabConfig import LabConfig
    port = LabConfig().get('ports','zlock')
    host = LabConfig().get('servers','zlock')
except (ImportError, IOError, ConfigParser.NoOptionError):
    print "Couldn't get host/port settings from LabConfig. Using localhost %d"%zlock.DEFAULT_PORT
    
print 'status of zlock server on %s %s is:'%(host, port)
zlock.connect(host, port)
print zlock.status()
