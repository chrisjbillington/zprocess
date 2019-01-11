from __future__ import unicode_literals, print_function, division
from subprocess import call, check_output, STDOUT
import sys
import os
import errno
from os.path import dirname


PYTHON2 = 'python2'
PYTHON3 = 'python3'


# The version we're testing:
PYTHONS = {'2': PYTHON2, '3': PYTHON3}


TEST_FILES = ['test_zlog_server.py', 'tests.py', 'test_zlock_server.py']
COVERAGE_PROCESS_START = os.path.abspath('.coveragerc')


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def get_python_version(executable):
    cmds = [executable, '--version']
    return check_output(cmds, stderr=STDOUT).decode('utf8').strip()


def get_python_full_executable(executable):
    cmds = [executable, '-c', 'import sys; print(sys.executable)']
    return check_output(cmds).decode('utf8').strip()


def get_user_site(executable):
    cmds = [executable, '-c', 'import site; print(site.getusersitepackages())']
    return check_output(cmds).decode('utf8').strip()


def get_coverage_path(executable):
    cmds = [executable, '-c', 'import coverage; print(coverage.__file__)']
    return check_output(cmds).decode('utf8').strip()


def run_tests(executable):

    version = get_python_version(executable)
    full_executable = get_python_full_executable(executable)

    print('doing tests for {}: {}'.format(full_executable, version))

    user_site = get_user_site(executable)
    if not os.path.exists(user_site):
        mkdir_p(user_site)
    path_file = os.path.join(user_site, 'coverage.pth')

    coverage_path = dirname(dirname(get_coverage_path(executable)))
    environ = os.environ.copy()
    environ['COVERAGE_PROCESS_START'] = COVERAGE_PROCESS_START

    try:
        with open(path_file, 'w') as f:
            f.write("import sys; sys.path.insert(0, '{}')\n".format(coverage_path))
            f.write("import coverage; coverage.process_startup()" + '\n')
        for test_file in TEST_FILES:
            call([executable, os.path.join('tests', test_file)], env=environ)
    finally:
        try:
            os.unlink(path_file)
        except OSError:
            pass


if __name__ == '__main__':
    for python_version in PYTHONS:
        run_tests(PYTHONS[python_version])
    try:
        call([sys.executable, '-m', 'coverage', 'combine'])
        call([sys.executable, '-m', 'coverage', 'html', '--rcfile=.coveragerc'])
        call([sys.executable, '-m', 'coverage', 'erase'])
    except Exception:
        pass
