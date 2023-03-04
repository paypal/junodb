#  
#  Copyright 2023 PayPal Inc.
#  
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#  
#     http://www.apache.org/licenses/LICENSE-2.0
#  
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#  
#  Package utility provides the utility interfaces for mux package
#  
#! /usr/bin/python
# 

from time import sleep, time
import json
import logging
import os
import signal
import socket
import subprocess
import sys

import cal
import util
from util import WorkerSherlock

json_file = ""


def setup_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d000 %(levelname)s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger

def get_my_host():
    return socket.gethostname().lower().split('.')[0]

def get_my_ip(host_name):
    return socket.gethostbyname_ex(host_name)[2]

# Check if ip is in use.
def ip_in_use(ip, port): 

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  
    try:
        s.connect((ip, port))
        s.close()
        return True
            
    except socket.error as e:
        pass     
    except Exception as e:
        pass
    
    return False
   
class Config():
    # Init default.
    def __init__(self):
        self.cfg_file = "./etcdsvr.txt"  
        self.client_port = 0
        self.peer_port = 0
        self.initial_cluster = ""
        self.is_existing_cluster = False
 
        self.peer_names = ""
        self.etcd_name = ""
        self.host_ip = ""
        self.peer_url = ""

        self.cluster_url = ""
        self.cluster_endpoints = ""
        self.data_dir = "."

        self.cal_host = "127.0.0.1"
        self.cal_port = 1118
        self.cal_enabled = True

    # Get members.
    def get_members(self):
        
        os.environ["ETCDCTL_API"] = "3"
        etcd_cmd = './etcdctl --endpoints="%s"' % (self.cluster_endpoints)
        cmd_list = "%s member list" % (etcd_cmd)
        members = ""
        try:
            members = subprocess.check_output(cmd_list, shell=True)
        except subprocess.CalledProcessError as e:
            pass
        return members 
 
    # Display member status
    def display_status(self):
        
        os.environ["ETCDCTL_API"] = "3"
        etcd_cmd = './etcdctl --endpoints="%s"' % (self.cluster_endpoints)
        cmd_list = "%s member list 2>&1 | cat" % (etcd_cmd)
        cmd_status = "%s endpoint status 2>&1 | cat" % (etcd_cmd)
        cmd_health = "%s endpoint health 2>&1 | cat" % (etcd_cmd)

        out = etcd_cmd
        out += "\n\n===== member list\n" + subprocess.check_output(cmd_list, shell=True)
        print(out)
        out = "===== endpoint status\n" + subprocess.check_output(cmd_status, shell=True)
        print(out)
        out = "===== endpoint health\n" + subprocess.check_output(cmd_health, shell=True) 
        print(out)
 
    # Join an existing cluster.
    def join_cluster(self):

        etcd_cmd = './etcdctl --endpoints="%s"' % (self.cluster_endpoints)
        cmd_select = "%s member list | grep '%s' | awk -F',' '{print $1}'" % (
            etcd_cmd, self.peer_url
        )

        cmd_add = "%s member add %s --peer-urls=%s" % (
            etcd_cmd, self.etcd_name, self.peer_url
        )

        cmd_rm = "/bin/rm -rf %s.etcd" % (self.etcd_name)

        ok = True
        err = None
        resp = ">> Members:\n"
        try:
            os.environ["ETCDCTL_API"] = "3"
            resp += self.get_members()

            hexid = ""

            # Remove the current entry if any
            resp += "\n>> Select:\n%s\n\n" % (cmd_select)
            hexid = subprocess.check_output(cmd_select, shell=True)

            if len(hexid) > 0:
                cmd_remove = "%s member remove %s" % (etcd_cmd, hexid)
                resp += "\n>> Remove:\n%s\n\n" % (cmd_remove)

                resp += subprocess.check_output(cmd_remove, stderr=subprocess.STDOUT, shell=True)
                sleep(5)

            # Add a new entry
            resp += "\n>> Add:\n%s\n\n" % (cmd_add)

            resp += subprocess.check_output(cmd_add, stderr=subprocess.STDOUT, shell=True)

            resp += "\n>> Members:\n"
            resp += self.get_members()
            resp += "\n"

            resp += cmd_rm
            resp += "\n"

        except subprocess.CalledProcessError as e:
            err = e.output
            ok = False

        print(resp)
        with open("join.log", "w+") as f:
            f.write(resp)
        
        if not ok:
            print(err + "\n[ERROR] Failed to join a cluster.")
            return

        os.system(cmd_rm)

    # Add a json config for etcd.
    def add_json_cfg(self):
        
        global json_file
        h = {}
    
        client_url = "%s:%d" % (self.host_ip, self.client_port)
        self.peer_url = "%s:%d" % (self.host_ip, self.peer_port)

	listen_client_urls = "http://0.0.0.0:%d" % (self.client_port)
        listen_peer_url = "http://0.0.0.0:%d" % (self.peer_port)

        h["advertise-client-urls"] = client_url
        h["initial-advertise-peer-urls"] = self.peer_url
        
        dir = self.etcd_name + ".etcd"
        if self.is_existing_cluster:
            # Join an existing cluster
            h["initial-cluster-state"] = "existing"
        h["initial-cluster"] = self.cluster_url
        h["listen-client-urls"] = listen_client_urls
        h["listen-peer-urls"] = listen_peer_url
        h["max-txn-ops"] = 2500
        h["strict-reconfig-check"] = False
        h["initial-cluster-token"] = "juno"
        h["name"] = self.etcd_name
        h["data-dir"] = self.data_dir + "/" + self.etcd_name + ".etcd"
    
        text = json.dumps(h, sort_keys=True, indent=4, 
                          separators=(',', ': '))
        print(text)
        json_file = self.etcd_name + ".json"
        with open(json_file, "w") as f:
            f.write(text)

    # Parse input config
    def parse_cfg(self, status_only):
    
        with open(self.cfg_file, 'r') as f:
            for line in f:
                line = line.replace('\n', '#')
                entry = line.split('#')[0]
                pair = entry.split('=')
                if len(pair) < 2:
                    continue
            
                key = pair[0].strip(' ')
                val = pair[1].strip(' ')
            
                if "etcdsvr.client_port" == key:
                    self.client_port = int(val)
                
                if "etcdsvr.peer_port" == key: 
                    self.peer_port = int(val)
                    
                if "etcdsvr.data_dir" == key:
                    self.data_dir = val
                    
                if "etcdsvr.peer_names" == key:
                    self.peer_names = val
                
                if "etcdsvr.initial_cluster" == key:
                    self.initial_cluster = val

                if "cal.host" == key:
                    self.cal_host = val

                if "cal.port" == key:
                    self.cal_port = int(val)

                if "cal.enabled" == key and val == "false":
                    self.cal_enabled = False
    
        name_list = self.peer_names.split('^')
        peer_hosts = self.initial_cluster.split('^')            
        host_name = get_my_host()
        host_ip_list = get_my_ip(host_name)
        
        list = []
        list_ip = []
        if len(name_list) != len(peer_hosts):
            return "[ERROR] Entry count mismatch between peer_names and initial_cluster."
            
        # Set endpints
        #==============
        for i in range(len(name_list)):
            if (peer_hosts[i].split('.')[0] == host_name or
                peer_hosts[i] in host_ip_list):
                    self.etcd_name = name_list[i]
                    self.host_ip = "http://%s" % (peer_hosts[i])
                    self.local_endpoint = "%s:%d" % (peer_hosts[i], self.peer_port)
 
            ip = "%s:%d" % (peer_hosts[i], self.peer_port)
            list_ip.append(ip)

        self.cluster_endpoints = ','.join(list_ip)
        if status_only:
            return None
 
        dir = self.etcd_name + ".etcd"
        members = ""
        if os.path.exists("join.log") and not os.path.exists(dir):
            self.is_existing_cluster = True
            members = self.get_members()
            if not members:
                return "[ERROR] Unable to access etcd."
            print("\n===== etcd members\n%s" % (members))

        # Set cluster url
        #=================
        for i in range(len(name_list)):
            if self.is_existing_cluster and not peer_hosts[i] in members:
                continue
            url = "%s=http://%s:%d" % (name_list[i], peer_hosts[i], self.peer_port)
            list.append(url)
    
        self.cluster_url = ','.join(list)
    
        if (self.client_port == 0 or
            self.peer_port == 0 or
            self.initial_cluster == "" or
            self.etcd_name == ""):
                return "[ERROR] Bad config in etcdsvr.txt."

        if ip_in_use(self.host_ip[7:], self.client_port):
            return "[ERROR] IP address at %s:%d is in use." % (self.host_ip[7:], self.client_port)

        if ip_in_use(self.host_ip[7:], self.peer_port):
            return "[ERROR] IP address at %s:%d is in use." % (self.host_ip[7:], self.peer_port)        
        
        self.add_json_cfg()
        return None
 

class Manager():
    def __init__(self, etcd_name, local_endpoint, cluster_endpoints, cal_enabled):
        self.logger = setup_logger("manager")
         
        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGHUP, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)
        signal.signal(signal.SIGQUIT, self.sig_handler)
        
        self.server = None
        self.pid = None
        self.etcd_name = etcd_name
        self.local_endpoint = local_endpoint
        self.cluster_endpoints = cluster_endpoints
        self.cal_enabled = cal_enabled
    
    def sig_handler(self, sig, frame):
        if not self.pid:
            return
        
        self.logger.info("[MANAGER] Signal %d received" %  (sig))
        util.quit_sherlock = True
        if self.cal_enabled:
            cal.event("MANAGER", "exit", 0, {"signal": sig})

        self.shutdown(0)
    
    def is_endpoint_healthy(self, slow_mode):
        os.environ["ETCDCTL_API"] = "3"
        etcd_cmd = './etcdctl --endpoints="%s"' % (self.local_endpoint)
        cmd_health = "%s endpoint health 2>&1 | cat" % (etcd_cmd)
        result = ""

        try_count = 10
        if slow_mode:
            try_count = 20 
        
        for i in range(try_count):
            sleep(1)
            if i > 5 and self.cal_enabled: 
                msg = "unhealthy_%s" % (self.etcd_name)
                cal.event("SERVER", msg, cal.ERROR, {})
            
            result = subprocess.check_output(cmd_health, shell=True)
            if "is healthy" in result:
                return True

        self.logger.info("[MANAGER] %s" % (result))
        print(result)
        return False

    # Start a child server    
    def start(self):
    
        global json_file
        cmd = "%s/etcdsvr_exe --config-file %s" % (os.getcwd(), json_file)
        print(cmd)
        self.logger.info("[MANAGER] %s" % (cmd))
        self.server = subprocess.Popen(cmd.split(' '), 
                                       shell=False, 
                                       stdin=subprocess.PIPE,
                                       preexec_fn=os.setpgrp)
        self.pid = self.server.pid

    def shutdown(self, status=0):
        util.quit_sherlock = True
        if self.pid:
            try:
                self.server.terminate()
                self.server.wait()
            except:
                pass
        if self.cal_enabled:
            cal.wait_and_close(timeout=5)

        sys.exit(status)

    def watch_and_recycle(self, slow_mode):
        
        try:
            count = 0
            start = time()
            
            worker = WorkerSherlock(self.logger, self.cluster_endpoints, self.cal_enabled)
            worker.start()
            while True:
               
                sleep(1)
                print("Starting etcd with quorum verification ...")
                self.logger.info("[MANAGER] Starting etcd ...")
                
                self.start()
                
                print(" ")
                self.logger.info("[MANAGER] Started etcd process %d" % (self.pid))
                if self.cal_enabled:
                   cal.event("SERVER", "start", 0, {'pid': self.pid})
                
                if not self.is_endpoint_healthy(slow_mode):
                    print("[ERROR] Failed to start etcd.")
                    self.shutdown(-1)

                print("Starting etcd process %d succeeded." % (self.pid))
                self.server.wait()

                # etcd server has exited.         
                sleep(1) 
                if self.cal_enabled:
                    cal.event("SERVER", "exit", cal.ERROR, {'pid': self.pid})
                count += 1
                if count < 30:
                    continue
                
                curr = time()
                if curr - start > 80:
                    count = 0
                    start = time()
                else:
                    self.logger.info("[MANAGER] etcd server thrashing.  Exit!")
                    if self.cal_enabled:
                        cal.event("SERVER", "thrashing", cal.ERROR, {})

                    self.shutdown(-1)

        except KeyboardInterrupt:

            self.logger.info("[MANAGER] Got interrupted.  Exit.")

            util.quit_sherlock = True
            if self.cal_enabled:
                cal.event("MANAGER", "interrupted", 0, {})

            self.shutdown(0)

        except Exception as e:
            
            self.logger.info("[MANAGER] Got exception %s.  Exit." % (e.message))
            
            util.quit_sherlock = True
            if self.cal_enabled:
                cal.event("MANAGER", "exception", cal.ERROR, {"exception": e.message})
                
            self.shutdown(0)
                
if __name__ == "__main__":
    
    with open("./etcdsvr.pid", "w") as f:
        f.write("%d\n" % (os.getpid()))

    cfg = Config()
    err = cfg.parse_cfg(False)
    if err:
        print(err)
        print("[ERROR] Failed to start etcd.")
        sys.exit(-1)

    my_pool = util.get_my_pool()
    cal.init(pool=my_pool, ip=cfg.cal_host, port=cfg.cal_port)
    
    mgr = Manager(cfg.etcd_name, cfg.local_endpoint, cfg.cluster_endpoints, cfg.cal_enabled)
    slow_mode = False
    if len(sys.argv) >= 2 and sys.argv[1] == "slow":
       slow_mode = True
    mgr.watch_and_recycle(slow_mode)
