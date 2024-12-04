import os
import binascii
import argparse

from zprocess.security import generate_shared_secret


def main():

    parser = argparse.ArgumentParser( description=""" zprocess.makesecret - this is a
        script to generate a shared secret file for use with zprocess security
        mechanisms. The contents of the file can be passed to
        zprocess.security.SecureContext(), or the filepath to the file passed as a
        command line argument to the zlog, zlock, or zprocess.remote servers.""" )

    parser.add_argument(
        'outfile',
        nargs='?',
        metavar='filename',
        default=None,
        help="""The file to write the shared_secret to. If not provided,
        a random output filename will be used.""",
    )

    args = parser.parse_args()

    shared_secret = generate_shared_secret()

    outfile = args.outfile
    if outfile is None:
        outfile = 'zpsecret-%s.key' % binascii.b2a_hex(os.urandom(4)).decode()
    with open(outfile, 'w') as f:
        f.write(shared_secret)
    print('wrote generated shared secret to:\n  %s' % os.path.abspath(outfile))

if __name__ == '__main__':
    main()
