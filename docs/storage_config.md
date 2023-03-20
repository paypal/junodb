[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
# Storage config file

Can be found in junodb/script/deploy/junostorageserv/config/config.toml


## Explanation of parameters that may be configured by the user. The values shown are the default values. 


* LogLevel="info" <br>
  Explanation: Sets the type of logs that should be displayed <br>
  Type: string <br>
  Options: "verbose" | "info" | "warning" | "error"<br>

* ShutdownWaitTime = "10s"<br>
  Explanation: Time to wait for the process to shutdown before it is forcefully shutdown<br>
  Type: golang time.Duration string <br>

* MaxTimeToLive = 259200<br>
  Explanation: Maximum time to live for junodb records in seconds <br>
  Type: integer <br>

* HttpMonAddr=":8089"<br>
  Explanation: <proxy_ip>:HttpMonAddr is the address for the storage monitoring page <br>
  Type: string <br>

* Under ClusterInfo<br>
  * NumZones=1<br>
    Explanation: Number of zones<br>
    Type: integer<br>

  * NumShards=1024<br>
    Explanation: Number of shards<br>
    Type: integer<br>
    
* Under DB<br>
  * Under DB.DbPaths<br>
    * Path="$PREFIX/rocksdb_$NAME/" <br>
        Explanation: Path to database folder<br>
        Type: string<br>


