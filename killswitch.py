import sys, os, time, signal

child_pid = int(sys.argv[1])
parent_pid = int(sys.argv[2])

if not os.name == 'nt':
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
else:

    import win32pdh, string, win32api

    def procids():
        #each instance is a process, you can have multiple processes w/same name
        junk, instances = win32pdh.EnumObjectItems(None,None,'process', win32pdh.PERF_DETAIL_WIZARD)
        proc_ids=[]
        proc_dict={}
        for instance in instances:
            if instance in proc_dict:
                proc_dict[instance] = proc_dict[instance] + 1
            else:
                proc_dict[instance]=0
        for instance, max_instances in proc_dict.items():
            for inum in xrange(max_instances+1):
                hq = win32pdh.OpenQuery() # initializes the query handle 
                path = win32pdh.MakeCounterPath( (None,'process',instance, None, inum,'ID Process') )
                counter_handle=win32pdh.AddCounter(hq, path) 
                try:
                    win32pdh.CollectQueryData(hq) #collects data for the counter 
                except Exception:
                    continue
                type, val = win32pdh.GetFormattedCounterValue(counter_handle, win32pdh.PDH_FMT_LONG)
                proc_ids.append((instance,str(val)))
                win32pdh.CloseQuery(hq) 

        proc_ids.sort()
        return [int(id) for name, id in proc_ids]
        
    while True:
        running_procs = procids()
        if not child_pid in running_procs:
            # The child is dead. Exit.
            break
        if not parent_pid in running_procs:
            # The parent is dead. Kill the child, then exit.
            os.kill(child_pid,signal.SIGTERM)
            break
        time.sleep(1)
