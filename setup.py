import os
from setuptools import setup

INSTALL_REQUIRES = [
    "pyzmq >=18.0",
    "ipaddress;         python_version == '2.7'",
    "subprocess32;      python_version == '2.7'",
    "enum34;            python_version == '2.7'",
    "pathlib;           python_version == '2.7'",
    "pywin32;           sys_platform == 'win32'",
    "windows-curses;    sys_platform == 'win32'",
    "importlib_metadata; python_version < '3.8'",
    "setuptools_scm >=4.1.0",
]

VERSION_SCHEME = {
    "version_scheme": "release-branch-semver",
    "local_scheme": os.getenv("SCM_LOCAL_SCHEME", "node-and-date"),
}

setup(
    name='zprocess',
    use_scm_version=VERSION_SCHEME,
    description="A set of utilities for multiprocessing using zeromq.",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Chris Billington',
    author_email='chrisjbillington@gmail.com',
    url='http://github.com/chrisjbillington/zprocess',
    license="BSD",
    packages=['zprocess'],
    zip_safe=False,
    setup_requires=['setuptools', 'setuptools_scm'],
    include_package_data=True,
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, !=3.5",
    install_requires=INSTALL_REQUIRES,
)
