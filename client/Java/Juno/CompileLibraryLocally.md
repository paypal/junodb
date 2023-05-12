<?xml version="1.0" encoding="UTF-8"?>
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
# Introduction
Before we can run and compile the Reference App, let's compile the Open Source Juno Libraries and Install them in our Repositories.

This is important as we pull the OpenSource Version of Juno.

## Quick Start
Please download the [OpenSource Juno](https://github.com/paypal/junodb/tree/dev/client/Java).  
Go to the Directory `~/junodb/client/Java/Juno` and perform the `mvn clean package` command (P. S. Skip Tests if you're in a hurry)

Please see that the jar files have been instantiated:
```
========== juno-client-api ==========

[INFO] --- maven-jar-plugin:2.4:jar (default-jar) @ juno-client-api ---
[INFO] Building jar: /Users/Documents/Github/junodb/client/Java/Juno/juno-client-api/target/juno-client-api-1.0.0.jar

========== juno-client-impl ==========

[INFO] --- maven-jar-plugin:2.4:jar (default-jar) @ juno-client-impl ---
[INFO] Building jar: /Users/Documents/Github/junodb/client/Java/Juno/juno-client-impl/target/juno-client-impl-1.0.0.jar
```

This Jars are very instrumental for our next steps! We've compiled the API Library and now can add to our Maven Repository.

Please go to the location where these Jars were created, mentioned in the previous step after `[INFO] Building jar`, and execute the following command to install these library to your Local Maven Repository

`mvn install:install-file -Dfile=[JARFILE] -DgroupId=[GROUP] -DartifactId=[ARTIFCACT ID] -Dversion=[VERSION] -Dpackaging=jar`

### !! Take note :  The DgroupId, DartifactId, and Dversion that you set here is what you'll pull into the Reference App's POM !! 

```
========== juno-client-api ==========

(base) <User> target % mvn install:install-file -Dfile=juno-client-api-1.0.0.jar -DgroupId=com.paypal.juno -DartifactId=juno-api -Dversion=1.0 -Dpackaging=jar
[INFO] Scanning for projects...
[INFO] 
[INFO] ------------------< org.apache.maven:standalone-pom >-------------------
[INFO] Building Maven Stub Project (No POM) 1
[INFO] --------------------------------[ pom ]---------------------------------
[INFO] 
[INFO] --- maven-install-plugin:2.4:install-file (default-cli) @ standalone-pom ---
[INFO] Installing /Users/Documents/Juno/juno-client-api/target/juno-client-api-1.0.0.jar to /Users/.m2/com/paypal/juno/juno-api/1.0/juno-api-1.0.jar
[INFO] Installing /var/folders/37/pmns3_5s1kbb1pzhxdg40nr00000gq/T/mvninstall3435520968944557452.pom to /Users/.m2/com/paypal/juno/juno-api/1.0/juno-api-1.0.pom
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  0.706 s
[INFO] Finished at: 2023-03-03T14:34:54-07:00
[INFO] ------------------------------------------------------------------------
(base) <User> target % 

========== juno-client-impl ==========

(base) <User> target % mvn install:install-file -Dfile=juno-client-impl-1.0.0.jar -DgroupId=com.paypal.juno -DartifactId=juno-impl -Dversion=2.0 -Dpackaging=jar
[INFO] Scanning for projects...
[INFO] 
[INFO] ------------------< org.apache.maven:standalone-pom >-------------------
[INFO] Building Maven Stub Project (No POM) 1
[INFO] --------------------------------[ pom ]---------------------------------
[INFO] 
[INFO] --- maven-install-plugin:2.4:install-file (default-cli) @ standalone-pom ---
[INFO] Installing /Users/Documents/Juno/juno-client-impl/target/juno-client-impl-1.0.0.jar to /Users/.m2/com/paypal/juno/juno-impl/2.0/juno-impl-2.0.jar
[INFO] Installing /var/folders/37/pmns3_5s1kbb1pzhxdg40nr00000gq/T/mvninstall6875939879921013650.pom to /Users/.m2/com/paypal/juno/juno-impl/2.0/juno-impl-2.0.pom
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  0.623 s
[INFO] Finished at: 2023-03-03T14:37:12-07:00
[INFO] ------------------------------------------------------------------------
```

Now, go to your .m2 folder, mine is on this path  `/Users/.m2/com/paypal/juno`, yours can be different, and it'll be on the output of the installation command .
```
(base) <User> juno % pwd
/Users/.m2/com/paypal/juno
(base) <User> juno % ls -l
total 0
drwxr-xr-x  5 <User>  staff  160 Mar  3 14:34 juno-api
drwxr-xr-x  6 <User>  staff  192 Mar  3 14:37 juno-impl

   |-juno-api
   |---1.0
   |-juno-impl
   |---2.0

```

The Maven Install commands is not great for Transitive Dependencies, which are dependencies that are important for the execution of our libraries. The maven install command creates a placeholder Pom in our repository, which will not pull the dependencies that are required to run our libraries. Please paste the dependencies from the respective POM files inside juno-client-impl/juno-client-api in these POM Files

<details>
<summary>POM File after Maven Install Command</summary>

```
========== juno-client-api ==========
(base) <User> 1.0 % pwd
/Users/.m2/com/paypal/juno/juno-api/1.0
(base) <User> 1.0 % cat juno-api-1.0.pom  
<project xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.paypal.juno</groupId>
  <artifactId>juno-api</artifactId>
  <version>1.0</version>
  <description>POM was created from install:install-file</description>
</project>

========== juno-client-impl ==========

(base) <User> 2.0 % pwd
/Users/.m2/com/paypal/juno/juno-impl/2.0
(base) <User> 2.0 % cat juno-impl-2.0.pom 
<project xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.paypal.juno</groupId>
  <artifactId>juno-impl</artifactId>
  <version>2.0</version>
  <description>POM was created from install:install-file</description>
</project>
```
</details>

Now Let's just import the dependencies from our original POM files that are in the library.

## !!Take note!!
We do however want juno-client-impl to point to the juno-client-api we have compiled locally, so the top dependency in the `/Users/.m2/com/paypal/juno/juno-impl/2.0/juno-impl-2.0.pom` will point to the [GROUP], [ARTIFCACT ID], and [VERSION] defined in the `mvn install:install-file` command used to compile the juno-client-api library, as done below.

<details>
<summary>What we want our Pom Files to be</summary>

```
========== juno-client-api ==========
(base) <User> 1.0 % pwd
/Users/.m2/com/paypal/juno/juno-api/1.0
(base) <User> 1.0 % cat juno-api-1.0.pom  
<project xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.paypal.juno</groupId>
  <artifactId>juno-api</artifactId>
  <version>1.0</version>
  <description>POM was created from install:install-file</description>
  
  <dependencies>
    <dependency>
        <groupId>io.reactivex</groupId>
        <artifactId>rxjava</artifactId>
        <version>1.3.8</version>
        <scope>compile</scope>
    </dependency>
    <dependency>
        <groupId>io.projectreactor</groupId>
        <artifactId>reactor-core</artifactId>
        <version>3.4.23</version>
        <scope>compile</scope>
    </dependency>
  </dependencies>
</project>
 
========== juno-client-impl ==========
 
(base) <User> 2.0 % pwd
/Users/.m2/com/paypal/juno/juno-impl/2.0
(base) <User> 2.0 % cat juno-impl-2.0.pom 
<project xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.paypal.juno</groupId>
  <artifactId>juno-impl</artifactId>
  <version>2.0</version>
  <description>POM was created from install:install-file</description>

  <dependencies>
    <dependency>
			<groupId>com.paypal.juno</groupId>
			<artifactId>juno-api</artifactId>
			<version>1.0</version>
		</dependency>
    <dependency>
			<groupId>io.netty</groupId>
			<artifactId>netty-buffer</artifactId>
			<version>4.1.82.Final</version>
		</dependency>
		<dependency>
			<groupId>io.netty</groupId>
			<artifactId>netty-codec</artifactId>
			<version>4.1.82.Final</version>
		</dependency>
		<dependency>
			<groupId>io.netty</groupId>
			<artifactId>netty-common</artifactId>
			<version>4.1.82.Final</version>
		</dependency>
		<dependency>
			<groupId>io.netty</groupId>
			<artifactId>netty-transport</artifactId>
			<version>4.1.82.Final</version>
		</dependency>
		<dependency>
			<groupId>io.netty</groupId>
			<artifactId>netty-handler</artifactId>
			<version>4.1.82.Final</version>
		</dependency>
		<dependency>
			<groupId>org.powermock</groupId>
			<artifactId>powermock-api-easymock</artifactId>
			<version>2.0.2</version>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>org.easymock</groupId>
			<artifactId>easymock</artifactId>
			<version>3.5</version>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>junit</groupId>
			<artifactId>junit</artifactId>
			<version>4.13.2</version>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>org.mockito</groupId>
			<artifactId>mockito-core</artifactId>
			<version>3.12.4</version>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>org.powermock</groupId>
			<artifactId>powermock-core</artifactId>
			<version>2.0.7</version>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>org.powermock</groupId>
			<artifactId>powermock-api-mockito2</artifactId>
			<version>2.0.7</version>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>org.powermock</groupId>
			<artifactId>powermock-module-junit4</artifactId>
			<version>2.0.7</version>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>org.springframework</groupId>
			<artifactId>spring-test</artifactId>
			<version>5.3.27</version>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>org.xerial.snappy</groupId>
			<artifactId>snappy-java</artifactId>
			<version>1.1.7.2</version>
		</dependency>
		<dependency>
			<groupId>javax.inject</groupId>
			<artifactId>javax.inject</artifactId>
			<version>1</version>
		</dependency>
		<dependency>
			<groupId>org.slf4j</groupId>
			<artifactId>slf4j-api</artifactId>
			<version>2.0.6</version>
		</dependency>
		<dependency>
			<groupId>org.slf4j</groupId>
			<artifactId>slf4j-simple</artifactId>
			<version>2.0.6</version>
		</dependency>
		<dependency>
			<groupId>commons-configuration</groupId>
			<artifactId>commons-configuration</artifactId>
			<version>1.10</version>
			<exclusions>
				<exclusion>
					<groupId>javax.inject</groupId>
					<artifactId>javax.inject</artifactId>
				</exclusion>
			</exclusions>
		</dependency>
		<dependency>
			<groupId>org.springframework</groupId>
			<artifactId>spring-beans</artifactId>
			<version>5.3.27</version>
		</dependency>
		<dependency>
			<groupId>commons-codec</groupId>
			<artifactId>commons-codec</artifactId>
			<version>1.15</version>
		</dependency>
		<dependency>
			<groupId>io.reactivex</groupId>
		    <artifactId>rxjava</artifactId>
			<version>1.3.8</version>
		</dependency>
		<dependency>
			<groupId>io.projectreactor</groupId>
			<artifactId>reactor-core</artifactId>
			<version>3.4.23</version>
		</dependency>
		<dependency>
			<groupId>org.springframework</groupId>
			<artifactId>spring-context</artifactId>
			<version>5.3.27</version>
		</dependency>
		<dependency>
			<groupId>io.micrometer</groupId>
			<artifactId>micrometer-core</artifactId>
			<version>1.9.4</version>
		</dependency>
		<dependency>
			<groupId>ch.qos.logback</groupId>
			<artifactId>logback-classic</artifactId>
			<version>1.2.11</version>
		</dependency>
  </dependencies>

</project>

```
</details>
