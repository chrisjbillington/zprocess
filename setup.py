from setuptools import setup
import os

try:
    from setuptools_conda import dist_conda
except ImportError:
    dist_conda = None

VERSION = '2.19.0.dev1'

# Auto generate a __version__ package for the package to import
with open(os.path.join('zprocess', '__version__.py'), 'w') as f:
    f.write("__version__ = '%s'\n" % VERSION)

INSTALL_REQUIRES = [
    "pyzmq >=18.0",
    "ipaddress;         python_version == '2.7'",
    "subprocess32;      python_version == '2.7'",
    "enum34;            python_version == '2.7'",
    "pywin32;           sys_platform == 'win32'",
    "windows-curses;    sys_platform == 'win32'",
]

setup(
    name='zprocess',
    version=VERSION,
    description="A set of utilities for multiprocessing using zeromq.",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Chris Billington',
    author_email='chrisjbillington@gmail.com',
    url='http://github.com/chrisjbillington/zprocess',
    license="BSD",
    packages=[
        'zprocess',
        'zprocess.zlock',
        'zprocess.locking',
        'zprocess.zlog',
        'zprocess.remote',
        'zprocess.examples',
        'zprocess.socks',
    ],
    zip_safe=False,
    setup_requires=['setuptools', 'setuptools_scm'],
    include_package_data=True,
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, !=3.5",
    install_requires=INSTALL_REQUIRES if 'CONDA_BUILD' not in os.environ else [],
    cmdclass={'dist_conda': dist_conda} if dist_conda is not None else {},
    command_options={
        'dist_conda': {
            'pythons': (__file__, ['3.6', '3.7', '3.8']),
            'platforms': (__file__, 'all'),
        },
    },
)
