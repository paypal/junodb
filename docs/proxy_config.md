# Proxy config file

Can be found in junodb/package_config/package/junoserv/config/config.toml


## Explanation of parameters that may be configured by the user. The values shown are the default values. 

* LogLevel="info" <br>
  Explanation: Sets the type of logs that should be displayed <br>
  Type: string <br>
  Options: "verbose" | "info" | "warning" | "error"<br>

* ShutdownWaitTime = "10s"<br>
  Explanation: Time to wait for the process to shutdown before it is forcefully shutdown<br>
  Type: golang time.Duration string <br>

* NumChildren = 8 <br> 
  Explanation: Number of child worker processes for proxy <br>
  Type: integer <br>

* MaxKeyLength = 256<br>
  Explanation: Maximum key length in bytes <br>
  Type: integer <br>

* MaxNamespaceLength = 64<br>
  Explanation: Maximum namespace length in bytes <br>
  Type: integer <br>

* MaxPayloadLength = 204800<br>
  Explanation: Maximum payload length in bytes <br>
  Type: integer <br>

* MaxTimeToLive = 259200<br>
  Explanation: Maximum time to live for junodb records in seconds <br>
  Type: integer <br>


* EtcdEnabled = true<br>
  Explanation: Set to true if etcd should be enabled <br>
  Type: boolean <br>


* HttpMonAddr=":8088"<br>
  Explanation: <proxy_ip>:HttpMonAddr is the address for the proxy monitoring page <br>
  Type: string <br>


* Under ClusterInfo<br>
  * NumZones=1<br>
    Explanation: Number of zones<br>
    Type: integer<br>

  * NumShards=1024<br>
    Explanation: Number of shards<br>
    Type: integer<br>
    

* Under Etcd<br>
  * Endpoints=["$STAGEIP:2379"]<br>
    Explanation: <ETCD IP>:<ETCD_port> is given here <br>
    Type:  Array of strings<br>

* Under Listener (TCP Port)
``` bash
 Addr = ":8080"
 ```
Explanation: Listener port without SSL <br>
Type:  string<br>

* Under Listener with SSL enabled (TLS Port)<br>
 ``` bash
 Addr = ":5080"
 SSLEnabled = true
 ```
  Explanation: Listener port with SSL <br>
  Type:  string for Addr, boolean for SSLEnabled<br>

