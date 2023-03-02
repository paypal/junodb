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
