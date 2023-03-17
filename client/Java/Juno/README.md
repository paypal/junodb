[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Juno Java SDK

This project is the client for Juno when working with Java (or on the JVM). It provides Juno DB operations through both asynchronous and synchronous APIs.
Features

    High-Performance Key/Value operations
    Asynchronous (through RxJava) and Synchronous APIs

Operations Supported

    Create - Inserts a key-value pair record into DB
    Get - Retrives an record that is stored in the DB 
    Update - Updates an existing record with the new value
    Set - Will update the record if found in DB else inserts the record into DB
    Destory - Deletes a record from DB
    CompareAndSet - Compares the version of the current record in the DB with the supplied version, the record will be updated only if the supplied version matches the version in DB.
    DoBatch - Will process a batch of requests. A list of requests has to be supplied for this API and will receivce a list of responses corresponding to the requests.
    
All the above operations are supported in Sync and Async formats. For Async format the DoBatch alone returns observable and rest of the operations return Single.

<details>
  <summary>Note: Please go through the Juno Client best practices and recommendations before integrating with Juno client</summary>

### Juno Client best practices and recommendations
Before integrating the Juno client please go over these best practices.

#### Concurrent updates on same record
Avoid updating the same record from different threads in the same instance or using different instances at the same time. This can cause a deadlock in Juno sever on certain conditions causing both the requests to fail with record locked error. If two or more Application instance trying to update the same record, one will succeed and other will fail with record locked error on steady state. Hence if an Application try to be conservative and update the same record with same data concurrently with multiple instances, do not retry the transaction as it will cause more record locked errors than success.

#### CompareAndSet API
The compareAndSet API should always be proceeded by a GET operation. The record context that has to be passed for a CompareAndSet has to be from the response of a Successful GET operation.

</details>

## Dependencies
Java 8 and Maven is required.

```shell
sudo apt install openjdk-8-jdk
sudo apt-get install maven
```


## Quick Start
The easiest way is to download the jar as well as its transitive dependencies through maven:
```
  <dependency>
      <groupId>com.paypal.juno</groupId>
      <artifactId>juno-client-api</artifactId>
  </dependency>
  <dependency>
      <groupId>com.paypal.juno</groupId>
      <artifactId>juno-client-impl</artifactId>
  </dependency>
```

## Configuration
The following are the Juno Client properties that an consumer has to supply to create the Juno Client object.
### Mandatory Paramaters
```
  juno.application_name=JunoTest          	//Name of the Application using this library
  juno.record_namespace=JunoNS            	//Record namespace where the record is to be stored
  juno.server.host=${junoserv-<pool>_host} 	//Host name/ VIP of Juno Server. pool - gen,risk,cookie,sess etc.
  juno.server.port=${junoserv-<pool>_port}     	//Connection port of Juno Server
```
### Optional Paramaters
```
  juno.default_record_lifetime_sec=1800         //Specify default lifetime for the operations - Deafult is 259200 sec
  juno.connection.timeout_msec=100         	//Client connection timeout to the Juno server - Default is 1000 msec
  juno.response.timeout_msec=200                //Response timeout in milli sec - Default is 1000 msec
  juno.useSSL=true			    	//To use SSL or not - Default is true
  juno.usePayloadCompression=true		//To compress the payload before sending to Juno server.Default is false.
  juno.operation.retry=true			//To retry an failed operation once. Default is false.
  juno.connection.byPassLTM=true		//To bypass the LTM for connections to Juno server. By defult its true from 2.1.0. 
```
## Inject Juno Client
```
Inject Juno client as below:

@Inject
private JunoClient junoClient;

@Inject
private JunoAsyncClient junoAsyncClient;
```
The easiest way to instantiate the JunoClient is using the @Inject method. If an app needs to use more than one Juno client, the standard @Named annotation can be used to differentiate between the two or more juno client beans, e.g., @Inject @Named("risk") JunoClient junoRiskClient . The Application property also should have the juno property prefixed with keyword risk, e.g., risk.juno.connection.timeout_msec=100, risk.juno.application_name = JunoRiskTest etc.

```agsl
@Inject                                 | Properties
private JunoClient junoClient;          | juno.default_record_lifetime_sec=1800
                                        | juno.record_namespace=JunoNS
                                        | juno.server.host=${junoserv-host}
                                        | juno.server.port=${junoserv-port}
                                        | juno.usePayloadCompression=true

@Inject                                 | Properties
@named("named1")                        | named1.juno.default_record_lifetime_sec=3200
private JunoClient junoNamed1Client;    | named1.juno.record_namespace=JunoRiskNS
                                        | named1.juno.server.host=${junoserv-host}
                                        | named1.juno.server.port=${junoserv-port}
                                        | named1.juno.connection.timeout_msec=100

@Inject                                 | Properties
@named("named2")                        | named2.juno.default_record_lifetime_sec=2400
private JunoClient junoNamed2Client;    | named2.juno.record_namespace=JunoSessionNS
                                        | named2.juno.server.host=${junoserv-host}
                                        | named2.juno.server.port=${junoserv-port}
                                        | named2.juno.response.timeout_msec=200
```
For this to work, we need a [spring-config](../examples/client/junoReferenceApp/junoreferenceAppService/src/main/resources/spring-client.xml) file and should import this file, please see the example [here](../examples/client/junoReferenceApp/junoreferenceAppService/src/main/java/com/juno/samples/JunoApplication.java)


## Instantiating the JunoClient using JunoClientFactory
Example of JunoClient without SSLContext and `useSSL=false`
```
URL url = this.getClass().getResource("/path/to/juno.properties");
Properties pConfig = new Properties();
pConfig.load(url.openStream());
junoClient = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig));
```

Example of JunoClient with SSLContext and `useSSL=true` with secrets in resources/secrets/
<br>Required secrets would be a `server.pem` and a `server.crt` file
```
URL url = this.getClass().getResource("/path/to/juno.properties");
Properties pConfig = new Properties();
pConfig.load(url.openStream());
junoClient = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig));
```

Example of JunoClient with SSLContext with your own secrets
```
import com.paypal.juno.util.juno.SSLUtil;

URL urlPem = SetTest.class.getResource("/path/to/*.pem");
URL urlCrt = SetTest.class.getResource("/path/to/*.crt");
URL url = this.getClass().getResource("/path/to/juno.properties");
Properties pConfig = new Properties();
pConfig.load(url.openStream());
junoClient = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext(urlCrt.getPath(), urlPem.getPath());
```

## Sample Code & Common errors

Please refer to [Juno Java Client](JunoJavaClient.md)\
Also, checkout FunctionalTests to see the various ways of Injecting and instantiating JunoClients
