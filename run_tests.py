from __future__ import unicode_literals, print_function, division
import subprocess
import sys
import os
import errno

PYTHON2 = 'python'
PYTHON3 = 'python3'

# The version we're testing:
PYTHONS = {'2': PYTHON2,
           '3': PYTHON3}


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

for python_version in PYTHONS:
    python_executable = PYTHONS[python_version]
    print('doing tests for python version {} with executable {}'.format(python_version, python_executable))
    cmds = [python_executable, '-c', 'import site; print(site.getusersitepackages())']
    user_site = subprocess.check_output(cmds).decode('utf8').strip()
    if not os.path.exists(user_site):
        mkdir_p(user_site)

    path_file = os.path.join(user_site, 'coverage.pth')

    COVERAGE_PROCESS_START = os.path.abspath('.coveragerc')
    import coverage
    coverage_import_path = os.path.dirname(os.path.dirname(os.path.abspath(coverage.__file__)))
    environ = os.environ.copy()
    environ['COVERAGE_PROCESS_START'] = COVERAGE_PROCESS_START

    try:
        with open(path_file, 'w') as f:
            f.write("import sys; sys.path.insert(0, '{}')\n".format(coverage_import_path))
            f.write("import coverage; coverage.process_startup()" + '\n')
        subprocess.call([python_executable, 'tests/tests.py'], env = environ)
    finally:
        try:
            os.unlink(path_file)
        except OSError:
            pass
try:
    subprocess.call([sys.executable, '-m', 'coverage', 'combine'])
    subprocess.call([sys.executable, '-m', 'coverage', 'html', '--rcfile=.coveragerc'])
    subprocess.call([sys.executable, '-m', 'coverage', 'erase'])
except Exception:
    pass
