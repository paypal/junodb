[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
# Config file for junocli and junoload 

## Sample Config file

You can copy the below sample config file when running junoload and change the AppName if required

```bash
#Name of the application 
AppName ="junos2"

[Sec]
  
  AppName = "junoserv"

  #Path to keystore
  KeyStoreFilePath = "./secrets/keystore.toml"
  
  #Path to server certificate
  CertPemFilePath  = "./secrets/server.crt"
  
  #Path to server pem file
  KeyPemFilePath   = "./secrets/server.pem"
  
  #Path to CA certificate
  CAFilePath = "./secrets/ca.crt"

```

## Explanation of config parameters
The complete list of parameters that can be customized in the config file is shown below with the default values if it is not explicitly set in the user defined config file

```bash
    #Name of the application
    Appname = "junoload"

    #Namespace for the application
    Namespace = "ns"

    #Number of times to retry if connection fails
    RetryCount = 1

    #TTL of entries
    DefaultTimeToLive = 1800

    #Total milliseconds after which connect operation fails
    ConnectTimeout = "100ms"

    #Total milliseconds after which read operation fails
    ReadTimeout = "500ms"

    #Total milliseconds after which write operation fails
    WriteTimeout = "500ms"

    #Total seconds after which request operation fails
    RequestTimeout = "1s"

    #Total seconds after which connection recycles
    ConnRecycleTimeout = "9s"

    #Payload length in bytes
    PayloadLen = 2048

    #TTL of entries 
    TimeToLive = 1800

    #Request pattern in terms of Create, Get, Update, Set, Destroy
    RequestPattern = "C:1,G:1,U:1,S:1,D:1"

    #Monitoring address for proxy monitor
    HttpMonAddr = ""

    #Number of executors to be running in parallel
    NumExecutor = 1

    #Number of requests per second
    NumReqPerSecond = 1000

    #Running time in seconds
    RunningTime = 100

    #Frequency with which to output statistic information in second
    StatOutputRate = 10
    
    [Server]
      #Proxy details
      #Addr = "<proxy_ip>:<proxy_port>
      Addr = "127.0.0.1:8080"
      Network = "" 
      SSLEnabled = false
    
    [Sec]
      #Security details
      AppName = "junoserv"
      CertPem = ""
      KeyPem = ""
      ClientAuth = true
      KeyStoreFilePath = ""
      CertPemFilePath = ""
      KeyPemFilePath = ""
      CAFilePath = ""
    
    
```
