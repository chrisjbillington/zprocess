import sys, os, time, signal

child_pid = int(sys.argv[1])
parent_pid = int(sys.argv[2])

while True:
    try:
        # Doesn't actually kill, just checks if the process ID exists
        os.kill(child_pid ,0)
    except OSError:
        # The child is dead. Exit.
        break
    try:
        os.kill(parent_pid,0)
    except OSError:
        # The parent is dead. Kill the child, then exit.
        os.kill(child_pid,signal.SIGKILL)
        break
    time.sleep(1)

