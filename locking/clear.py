import sys
import zlock

clear_all = False

if 'all' in sys.argv:
    clear_all = True
    sys.argv.remove('all')
    
if __name__ == '__main__':
    host, port = zlock.guess_server_address()   
    print 'Clearing %s locks on zlock server %s %s:'%('*all*' if clear_all else 'expired', host, port)
    zlock.connect(host, port)
    zlock.clear(clear_all)
    print zlock.status()
