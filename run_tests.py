from __future__ import unicode_literals, print_function, division
from subprocess import call, check_output, STDOUT
import sys
import os
import errno
from os.path import dirname


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


def run_tests():
    executable = sys.executable
    version = get_python_version(executable)
    full_executable = get_python_full_executable(executable)

    print('doing tests for {}: {}'.format(full_executable, version))

    environ = os.environ.copy()
    environ['COVERAGE_PROCESS_START'] = COVERAGE_PROCESS_START
    environ['PYTHONPATH'] = os.getcwd()

    SITECUSTOMIZE = os.path.abspath('sitecustomize.py')
    try:
        with open(SITECUSTOMIZE, 'w') as f:
            f.write("import coverage; coverage.process_startup()")

        for test_file in TEST_FILES:
            rc = call([executable, os.path.join('tests', test_file)], env=environ)
            if rc:
                sys.exit(rc)
    finally:
        try:
            os.unlink(SITECUSTOMIZE)
        except Exception:
            pass

if __name__ == '__main__':
    run_tests()
    # try:
    #     # call([sys.executable, '-m', 'coverage', 'combine'])
    #     # call([sys.executable, '-m', 'coverage', 'html', '--rcfile=.coveragerc'])
    # except Exception:
    #     pass
