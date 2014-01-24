import zlock
import sys

if __name__ == '__main__':
    host, port = zlock.guess_server_address()   
    print 'status of zlock server on %s %s is:'%(host, port)
    zlock.connect(host, port)
    print zlock.status()
