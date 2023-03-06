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
 
#!/x/opt/pp/bin/python
#

import os
import sys

import etcdsvr

if __name__ == "__main__":
     
     if len(sys.argv) == 1:
         sys.exit(0)

     cfg = etcdsvr.Config()
     if sys.argv[1] == "status":
         err = cfg.parse_cfg(True)
         if err:
            print(err)
            sys.exit(-1)

         cfg.display_status()
     
     if sys.argv[1] == "join":
         dir = os.path.dirname(os.path.realpath(sys.argv[0]))
         if not os.access(dir, os.W_OK):
             print('[ERROR] Permission denied.')
             sys.exit(-1)
         
         err = cfg.parse_cfg(False)
         if err:
            print(err)
            sys.exit(-1)

         cfg.join_cluster()
