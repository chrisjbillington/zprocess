#####################################################################
#                                                                   #
# __main__.py                                                       #
#                                                                   #
# Copyright 2013, Chris Billington                                  #
#                                                                   #
# This file is part of the zprocess project (see                    #
# https://bitbucket.org/cbillington/zprocess) and is licensed under #
# the Simplified BSD License. See the license.txt file in the root  #
# of the project for the full license.                              #
#                                                                   #
#####################################################################
from __future__ import division, unicode_literals, print_function, absolute_import
import sys
import os
import binascii
import argparse

# Ensure zprocess is in the path if we are running from this directory
if os.path.abspath(os.getcwd()) == os.path.dirname(os.path.abspath(__file__)):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.getcwd())))

from zprocess.security import generate_shared_secret

"""This is a script to generate a shared secret file for use with zprocess security
mechanisms"""

def main():

    parser = argparse.ArgumentParser(description="zprocess.makesecret.")

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