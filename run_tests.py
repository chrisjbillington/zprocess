import subprocess
import sys
import os
import site
user_site = site.getusersitepackages()
path_file = os.path.join(user_site, 'coverage.pth')

COVERAGE_PROCESS_START = os.path.abspath('.coveragerc')
environ = os.environ.copy()
environ['COVERAGE_PROCESS_START'] = COVERAGE_PROCESS_START

subprocess.call([sys.executable, '-m', 'coverage', 'erase'])

try:
    with open(path_file, 'w') as f:
        f.write("import coverage; coverage.process_startup()" + '\n')
    subprocess.call([sys.executable, 'tests/tests.py'], env = environ)
finally:
    try:
        subprocess.call([sys.executable, '-m', 'coverage', 'combine'])
        subprocess.call([sys.executable, '-m', 'coverage', 'html', '--rcfile=.coveragerc'])
    except Exception:
        pass
    try:
        os.unlink(path_file)
    except OSError:
        pass
