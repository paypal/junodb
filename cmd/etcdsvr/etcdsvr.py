#!/usr/bin/python
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


from time import sleep, time
import json
import logging
import os
import random
import shlex
import signal
import socket
import subprocess
import sys

json_file = ""
h = {}
restartCount = 0

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
            
    except socket.error:
        pass     
    except Exception:
        pass
    
    return False
 
def run_cmd(cmd, get_result=False):
    result = ""
    try:
        out = None
        if get_result:
            out = subprocess.PIPE
        re = subprocess.run(shlex.split(cmd), stdout=out, stderr=out, 
                            universal_newlines=True, check=True)
        result = str(re.stdout)
       
    except subprocess.CalledProcessError:
        pass
    return result    
 
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

    # Get members.
    def get_members(self):
        
        os.environ["ETCDCTL_API"] = "3"
        etcd_cmd = '%s/etcdctl --endpoints="%s"' % (os.getcwd(), self.cluster_endpoints)
        cmd_list = "%s member list" % (etcd_cmd)
        members = ""
        try:
            members = run_cmd(cmd_list, get_result=True) 
        except subprocess.CalledProcessError:
            pass
        return members 
 
    # Display member status
    def display_status(self):
        
        os.environ["ETCDCTL_API"] = "3"
        etcd_cmd = '%s/etcdctl --endpoints="%s"' % (os.getcwd(), self.cluster_endpoints)
        cmd_list = "%s member list" % (etcd_cmd)
        cmd_status = "%s endpoint status" % (etcd_cmd)
        cmd_health = "%s endpoint health" % (etcd_cmd)

        print(etcd_cmd + "\n\n===== member list\n")
        run_cmd(cmd_list) 
        
        print("\n===== endpoint status\n")
        run_cmd(cmd_status) 
        
        print("\n===== endpoint health\n") 
        run_cmd(cmd_health) 
 
    # Join an existing cluster.
    def join_cluster(self):

        etcd_cmd = '%s/etcdctl --endpoints="%s"' % (os.getcwd(), self.cluster_endpoints)

        cmd_add = "%s member add %s --peer-urls=%s" % (
            etcd_cmd, self.etcd_name, self.peer_url
        )

        cmd_rm = "/bin/rm -rf %s.etcd" % (self.etcd_name)

        ok = True
        err = None
        try:
            os.environ["ETCDCTL_API"] = "3"
            text = self.get_members()

            hexid = ""
            resp = ">> Members:\n" + text
            print(resp)
            
            # Remove the current entry if any
            lines = text.split("\n")
            for li in lines:
                tokens = li.split(", ")
                if len(tokens) > 3 and self.etcd_name == tokens[2]:
                    hexid = tokens[0]
                    break

            if len(hexid) > 0:
                cmd_remove = "%s member remove %s\n\n" % (etcd_cmd, hexid)
                print("\n>> Remove:\n%s" % (cmd_remove))
                resp += cmd_remove
                
                run_cmd(cmd_remove) 
                sleep(5)

            # Add a new entry
            msg = "\n>> Add:\n%s\n\n" % (cmd_add)
            print(msg)
            resp += msg

            run_cmd(cmd_add) 
            msg = "\n>> Members:\n" + self.get_members()
            print(msg)
            resp += msg
            
            msg = "\n" + cmd_rm + "\n"
            print(msg)
            resp += msg

        except subprocess.CalledProcessError as e:
            err = e.output
            ok = False

        with open("join.log", "w+") as f:
            f.write(resp)
        
        if not ok:
            print(err + "\n[ERROR] Failed to join a cluster.")
            return False

        os.system(cmd_rm)
        return True

    # Add a json config for etcd.
    def add_json_cfg(self):
        
        global json_file, h
        h = {}
    
        client_url = "%s:%d" % (self.host_ip, self.client_port)
        self.peer_url = "%s:%d" % (self.host_ip, self.peer_port)
        h["advertise-client-urls"] = client_url
        h["initial-advertise-peer-urls"] = self.peer_url
        
        if self.is_existing_cluster:
            # Join an existing cluster
            h["initial-cluster-state"] = "existing"
        h["initial-cluster"] = self.cluster_url
        h["listen-client-urls"] = client_url
        h["listen-peer-urls"] = self.peer_url
        h["max-txn-ops"] = 6000
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

    def fix_json_cfg(self):
        
        global json_file, h
        h["initial-cluster-state"] = "existing"
        
        text = json.dumps(h, sort_keys=True, indent=4, 
                          separators=(',', ': '))
        with open(json_file, "w") as f:
            f.write(text)
        
    # Parse input config
    def parse_cfg(self, status_only):
    
        global restartCount
        
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
        if not os.path.exists(dir):
            restartCount = 2
            
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
    def __init__(self, etcd_name, local_endpoint, cluster_endpoints):
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
    
    def sig_handler(self, sig, frame):
        if not self.pid:
            return
        
        self.logger.info("[MANAGER] Signal %d received" %  (sig))
        self.shutdown(0)
    
    def is_endpoint_healthy(self, wait_time):
        os.environ["ETCDCTL_API"] = "3"
        etcd_cmd = '%s/etcdctl --endpoints="%s"' % (os.getcwd(), self.local_endpoint)
        cmd_health = "%s endpoint health" % (etcd_cmd)
        result = ""
        
        now = int(time())
        for i in range(10):
            sleep(5)
            t = int(time()) - now
            if t > wait_time:
                break
            
            if t > 50: 
                msg = "unhealthy_%s retry ..." % (self.etcd_name)
                self.logger.error("[MANAGER] %s" % (msg))
            
            result = run_cmd(cmd_health, get_result=True) 
            if "is health" in result:
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
        if self.pid:
            try:
                self.logger.info("[MANAGER] shutdown %d" % (self.pid))
                self.server.terminate()
                self.server.wait()
            except:
                pass

        sys.exit(status)
        
    def shutdown_server(self):
        if not self.pid:
            return
            
        try:
            self.logger.info("[MANAGER] shutdown %d" % (self.pid))
            self.server_terminate()
            self.server_wait()
        except:
            pass
        
        self.pid = None    

    def watch_and_recycle(self, cfg):
        
        global restartCount
        
        try:
            count = 0
            start = time()
            
            while True:
               
                sleep(1)
                print("Starting etcd with quorum verification ...")
                self.logger.info("[MANAGER] Starting etcd ...")
                
                self.start()
                
                print(" ")
                self.logger.info("[MANAGER] Started etcd process %d" % (self.pid))
                
                wait_time = 60 + random.randint(0,10)
                while False: #not self.is_endpoint_healthy(wait_time):
                    
                    if restartCount > 0:
                        self.shutdown_server()
                        cfg.fix_json_cfg()
                        
                        if cfg.join_cluster():
                            self.logger.info("[MANAGER] Starting etcd ...")
                            self.start()
                            restartCount -= 1
                            wait_time = 20 + random.randint(0,15)
                            continue
                    
                    print("[ERROR] Failed to start etcd.")
                    self.shutdown(-1)   # exit

                print("Starting etcd process %d succeeded." % (self.pid))
                if os.path.exists("join.log"): 
                    os.remove("join.log")
                self.server.wait()

                # etcd server has exited.         
                sleep(1) 
                count += 1
                if count < 30:
                    continue
                
                curr = time()
                if curr - start > 80:
                    count = 0
                    start = time()
                else:
                    self.logger.error("[MANAGER] etcd server thrashing.  Exit!")

                    self.shutdown(-1)

        except KeyboardInterrupt:

            self.logger.info("[MANAGER] Got interrupted.  Exit.")

            self.shutdown(0)

        except Exception as e:
            
            self.logger.info("[MANAGER] Got exception %s.  Exit." % (e.message))
                
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
    
    mgr = Manager(cfg.etcd_name, cfg.local_endpoint, cfg.cluster_endpoints)
    slow_mode = False
  
    mgr.watch_and_recycle(cfg)
