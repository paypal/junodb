[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Sample Juno
If you're compiling the Juno Libraries locally, please complete the [Prerequisite - how to compile library locally](../../../Juno/CompileLibraryLocally.md)

## Dependencies
Java 8 and Maven is required.

```shell
sudo apt install openjdk-8-jdk
sudo apt-get install maven
```

## Quick Start
In the [junoreferenceAppService POM](junoreferenceAppService/pom.xml), add the dependency of the library as such. 
<br>We're using the GroupID, ArtifactID and VersionID from the [Official Juno Library on Maven]().

```
    <dependency>
      <groupId>com.paypal.juno</groupId>
      <artifactId>juno-client-api</artifactId>
      <version>1.0.0</version>
    </dependency>
    <dependency>
      <groupId>com.paypal.juno</groupId>
      <artifactId>juno-client-impl</artifactId>
      <version>1.0.0</version>
    </dependency>
```

But if installing the library locally then use the [GROUP], [ARTIFCACT ID], and [VERSION] defined in the [Prerequisite](../../../Juno/CompileLibraryLocally.md) installation step.
```
    <dependency>
      <groupId>com.paypal.juno</groupId>
      <artifactId>juno-api</artifactId>
      <version>1.0</version>
    </dependency>
    <dependency>
      <groupId>com.paypal.juno</groupId>
      <artifactId>juno-impl</artifactId>
      <version>2.0</version>
    </dependency>
```

### Generating Secrets
For the TLS/SSL Handshake with the Server, we need to have the Private Keys of the System for the Client to use. <br> 
Please execute the [gensecrets.sh](junoreferenceAppService/src/main/resources/secrets/gensecrets.sh) file, inside the [resources/secret](junoreferenceAppService/src/main/resources/secrets) folder, to generate the required secrets for your system by executing the following commands:

```agsl
chmod u+x gensecrets.sh
./gensecrets.sh
```

### IDE

Open the repo in your favourite IDE, inside the [resources](junoreferenceAppService/src/main/resources) folder, please put the [Juno Properties](junoreferenceAppService/src/main/resources/application.properties) in 
`application.properties` and have your secrets `server.pem server.crt` in the `resources/secret` folder, run the `JunoApplication`. 

### CLI

Go to the root of the directory `~/junoReferenceApp` and perform `mvn clean package`

Then, go to the target directory `~/junoReferenceApp/junoreferenceAppService/target` and copy your [resources](junoreferenceAppService/src/main/resources) folder here alongside the jar file created, like such

```
   |-junoreferenceAppService-0.0.1-SNAPSHOT.jar
   |-resources
   |---application.properties
   |---secrets
   |------server.crt
   |------server.pem
   
```
Then run the jar using the following command, please make sure the [application properties has all the mandatory fields](https://github.com/paypal/junodb/tree/dev/client/Java/Juno#mandatory-paramaters) present and the secrets folder has the server.crt and server.pem

Please make sure that the jar file created is on the same level as the resources for this command to properly execute,
```
java -cp 'junoreferenceAppService-0.0.1-SNAPSHOT.jar:resources/' com.juno.samples.JunoApplication
```
OR you can provide the location to your Resources to the jar like such:
```
java -cp 'junoreferenceAppService-0.0.1-SNAPSHOT.jar:/path/to/resources/' com.juno.samples.JunoApplication
```

## Operations Supported
Supports all Sync/ Async Operations mentioned in the [Official Juno Documentation](https://github.com/paypal/junodb/blob/dev/client/Java/Juno/README.md)

### Endpoints
<details>
  <summary>Sync Api</summary>

```
http://localhost:8080/samplejuno/recordcreate

    @POST
    @PostMapping("/recordcreate")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> recordCreate(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;


http://localhost:8080/samplejuno/recordcreatettl

    @POST
    @PostMapping("/recordcreatettl")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.APPLICATION_JSON })
    ResponseEntity<String> recordCreate(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

http://localhost:8080/samplejuno/recordget/{key}

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @GetMapping("/recordget/{key}")
    ResponseEntity<String> recordGet(@PathVariable String key) throws JunoException, InterruptedException;

http://localhost:8080/samplejuno/recordgetttl/{key}/{ttl}

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @GetMapping("/recordgetttl/{key}/{ttl}")
    ResponseEntity<String> recordGet(@PathVariable("key") String key, @PathVariable("ttl") Long ttl) throws JunoException, InterruptedException;

http://localhost:8080/samplejuno/recordupdate

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/recordupdate")
    ResponseEntity<String> recordUpdate(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

http://localhost:8080/samplejuno/recordupdatettl

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/recordupdatettl")
    ResponseEntity<String> recordUpdate(@FormParam("key")  String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

http://localhost:8080/samplejuno/recordset

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/recordset")
    ResponseEntity<String> recordSet(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

http://localhost:8080/samplejuno/recordsetttl

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/recordsetttl")
    ResponseEntity<String> recordSet(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;


http://localhost:8080/samplejuno/recordcompareandset

    @POST
    @PostMapping("/recordcompareandset")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> recordCompareAndSet(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;


http://localhost:8080/samplejuno/recordcompareandsetttl

    @POST
    @PostMapping("/recordcompareandsetttl")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> recordCompareAndSetTTL(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

http://localhost:8080/samplejuno/recorddelete/{key}

    @DELETE
    @Produces({ MediaType.APPLICATION_JSON })
    @DeleteMapping("/recorddelete/{key}")
    ResponseEntity<String> recordDelete(@PathVariable String key) throws JunoException;
   
```
</details>

<details>
  <summary>Async Mono</summary>

```
http://localhost:8080/samplejuno/reactcreate

    @POST
    @PostMapping("/reactcreate")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> reactCreate(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;


http://localhost:8080/samplejuno/reactcreatettl

    @POST
    @PostMapping("/reactcreatettl")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.APPLICATION_JSON })
    ResponseEntity<String> reactCreate(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

http://localhost:8080/samplejuno/reactget/{key}

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @GetMapping("/reactget/{key}")
    ResponseEntity<String> reactGet(@PathVariable String key) throws JunoException, InterruptedException;

http://localhost:8080/samplejuno/reactgetttl/{key}/{ttl}

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @GetMapping("/reactgetttl/{key}/{ttl}")
    ResponseEntity<String> reactGet(@PathVariable("key") String key, @PathVariable("ttl") Long ttl) throws JunoException, InterruptedException;

http://localhost:8080/samplejuno/reactupdate

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/reactupdate")
    ResponseEntity<String> reactUpdate(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

http://localhost:8080/samplejuno/reactupdatettl

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/reactupdatettl")
    ResponseEntity<String> reactUpdate(@FormParam("key")  String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

http://localhost:8080/samplejuno/reactset

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/reactset")
    ResponseEntity<String> reactSet(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

http://localhost:8080/samplejuno/reactsetttl

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/reactsetttl")
    ResponseEntity<String> reactSet(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;


http://localhost:8080/samplejuno/reactcompareandset

    @POST
    @PostMapping("/reactcompareandset")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> reactCompareAndSet(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;


http://localhost:8080/samplejuno/reactcompareandsetttl

    @POST
    @PostMapping("/reactcompareandsetttl")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> reactCompareAndSetTTL(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

http://localhost:8080/samplejuno/reactdelete/{key}

    @DELETE
    @Produces({ MediaType.APPLICATION_JSON })
    @DeleteMapping("/reactdelete/{key}")
    ResponseEntity<String> reactDelete(@PathVariable String key) throws JunoException;
```
</details>

<details>
  <summary>Async Rx-Java</summary>

```
http://localhost:8080/samplejuno/asynccreate

    @POST
    @PostMapping("/asynccreate")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> asyncCreate(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;


http://localhost:8080/samplejuno/asynccreatettl

    @POST
    @PostMapping("/asynccreatettl")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.APPLICATION_JSON })
    ResponseEntity<String> asyncCreate(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

http://localhost:8080/samplejuno/asyncget/{key}

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @GetMapping("/asyncget/{key}")
    ResponseEntity<String> asyncGet(@PathVariable String key) throws JunoException, InterruptedException;

http://localhost:8080/samplejuno/asyncgetttl/{key}/{ttl}

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @GetMapping("/asyncgetttl/{key}/{ttl}")
    ResponseEntity<String> asyncGet(@PathVariable("key") String key, @PathVariable("ttl") Long ttl) throws JunoException, InterruptedException;

http://localhost:8080/samplejuno/asyncupdate

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/asyncupdate")
    ResponseEntity<String> asyncUpdate(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

http://localhost:8080/samplejuno/asyncupdatettl

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/asyncupdatettl")
    ResponseEntity<String> asyncUpdate(@FormParam("key")  String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

http://localhost:8080/samplejuno/asyncset

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/asyncset")
    ResponseEntity<String> asyncSet(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;

http://localhost:8080/samplejuno/asyncsetttl

    @PUT
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes("application/x-www-form-urlencoded")
    @PutMapping("/asyncsetttl")
    ResponseEntity<String> asyncSet(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;


http://localhost:8080/samplejuno/asynccompareandset

    @POST
    @PostMapping("/asynccompareandset")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> asyncCompareAndSet(@FormParam("key") String key, @FormParam("value") String value) throws JunoException;


http://localhost:8080/samplejuno/asynccompareandsetttl

    @POST
    @PostMapping("/asynccompareandsetttl")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({ MediaType.TEXT_PLAIN })
    ResponseEntity<String> asyncCompareAndSetTTL(@FormParam("key") String key, @FormParam("value") String value, @FormParam("ttl") Long ttl) throws JunoException;

http://localhost:8080/samplejuno/asyncdelete/{key}

    @DELETE
    @Produces({ MediaType.APPLICATION_JSON })
    @DeleteMapping("/asyncdelete/{key}")
    ResponseEntity<String> asyncDelete(@PathVariable String key) throws JunoException;
```
</details>

### Example

POST Request

```
curl --location --request POST 'http://localhost:8080/samplejuno/asynccreate' \
--header 'Content-Type: application/x-www-form-urlencoded' \
--data-urlencode 'key=test1' \
--data-urlencode 'value=value' \
--data-urlencode 'ttl=1800'
```


Get Request 

```
curl --location --request GET 'http://localhost:8080/samplejuno/asyncget/test1'
```

Put Request

```
curl --location --request PUT 'http://localhost:8080/samplejuno/asyncset' \
--header 'Content-Type: application/x-www-form-urlencoded' \
--data-urlencode 'key=test1' \
--data-urlencode 'value=value'
```

 Delete Request 

```
curl --location --request DELETE 'http://localhost:8080/samplejuno/asyncdelete/test1'
```


