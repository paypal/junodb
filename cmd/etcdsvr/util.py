#!/x/opt/pp/bin/python
# 

from time import sleep, time
import os
import socket
import subprocess
import threading

import cal

frontier_phx   = "frontierproxy-vip.phx.paypal.com"

frontier_ccg13 = "frontierproxy-vip.ccg13.slc.paypalinc.com"
frontier_ccg14 = "frontierproxy-vip.ccg14.slc.paypalinc.com"
frontier_ccg23 = "frontierproxy-vip.ccg23.lvs.paypalinc.com"
frontier_sb    = "node-sherlockconverter-vip.sandbox5.ccg18.slc.paypalinc.com"

frontier_ccg71 = "frontierproxy-vip.ccg71.sha.gopayinc.com.cn"
frontier_ccg75 = "frontierproxy-vip.ccg75.pkx.gopayinc.com.cn"

frontier_stage = "msm-sherlock-metrics-converter-paypal-observability.us-central1.gcp.dev.paypalinc.com"

quit_sherlock = False
my_host = ""
CYCLE = 61

def get_my_pool():
    pool = os.getcwd().split('/')[-1]
    default= "junoclusterserv-gen"
    
    if pool[:15] != default[:15]:
        pool = default
    
    return pool

def get_frontier_endpoint():
    
    global my_host
    my_host = socket.gethostname().lower().split('.')[0]
    prefix = ' ' + my_host[0:5] + ' '    
    
    if (my_host[0:3] == "phx" or
        prefix in  " ccg01 dcg01 dcg02 raz01 "):
        return frontier_phx
            
    elif prefix in " ccg11 dcg12 ":
        return frontier_ccg13
    
    elif prefix in " ccg12 dcg11 dcg13 dcg14 ":
        return frontier_ccg13

    elif (my_host[0:5] == "ccg13"):
        return frontier_ccg13

    elif (my_host[0:5] == "ccg14"):
        return frontier_ccg14

    elif (my_host[0:5] == "ccg23"):
        return frontier_ccg23
    
    elif (my_host[0:5] == "ccg18"):
        return frontier_sb
       
    elif (my_host[0:5] == "ccg71"):
        return frontier_ccg71

    elif (my_host[0:5] == "ccg75"):
        return frontier_ccg75

    else:
        pass
    
    return frontier_stage

# Sherlock thread
class WorkerSherlock(threading.Thread):
    def __init__(self, logger, endpoints, cal_enabled):
        threading.Thread.__init__(self)
        
        self.logger = logger
        self.cluster_endpoints = endpoints
        self.cal_enabled = cal_enabled
    
        self.my_pool = get_my_pool()
        self.frontier_endpoint = get_frontier_endpoint()
 
    def run(self):
        global quit_sherlock, CYCLE
        
        self.logger.info("sherlock thread starts.")
        
        while True:
            for i in range(CYCLE):
                sleep(1)
                if quit_sherlock:
                    return
                
            count = self.check_status()
            self.logger.info("active_count=%d" % (count))
            self.logger.handlers[0].flush()
            if self.cal_enabled:
                cal.event("SERVER", "status", 0, {'active': str(count)})

    def check_status(self):
        global my_host
        
        tail = 'endpoint status | egrep "(, true,|, false,)"| wc -l'
        cmd = './etcdctl --endpoints="%s" %s' % (self.cluster_endpoints, tail) 

        try:
            os.environ["ETCDCTL_API"] = "3"
            p = subprocess.Popen(cmd, shell=True, stdin=None, stdout=subprocess.PIPE)
    
            resp= int(p.stdout.read())
            
            if not os.path.isfile("./sherlock"):
                return resp
                
            cmd = './sherlock -a %d -e "%s" -p %s -h %s' % (
                    resp, self.frontier_endpoint, 
                    self.my_pool, my_host)
                    
            self.logger.info("%s" % (cmd[11:]))
            self.logger.handlers[0].flush()
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
 
            return resp
        
        except Exception as e:
            self.logger.info(e.message)
            return 0
            

