# Employee Dashboard App

## Tested Using
- Maven version 3.8.1
- Java version 11
- Docker version 20.10.14
- Docker Compose version v2.5.1
```bash
mvn -version
Apache Maven 3.8.1 (05c21c65bdfed0f71a2f2ada8b84da59348c4c5d)
Maven home: /opt/apache-maven-3.8.1
Java version: 11.0.10, vendor: Azul Systems, Inc., runtime: /Library/Java/JavaVirtualMachines/zulu-sa-11.0.10.jdk/Contents/Home
Default locale: en_US, platform encoding: UTF-8
OS name: "mac os x", version: "10.16", arch: "x86_64", family: "mac"

docker version 
Client:
 Cloud integration: v1.0.24
 Version:           20.10.14
 API version:       1.41
 Go version:        go1.16.15
 Git commit:        a224086
 Built:             Thu Mar 24 01:49:20 2022
 OS/Arch:           darwin/amd64
 Context:           default
 Experimental:      true

Server: Docker Desktop 4.8.2 (79419)
 Engine:
  Version:          20.10.14
  API version:      1.41 (minimum version 1.12)
  Go version:       go1.16.15
  Git commit:       87a90dc
  Built:            Thu Mar 24 01:46:14 2022
  OS/Arch:          linux/amd64
  Experimental:     false
 containerd:
  Version:          1.5.11
  GitCommit:        3df54a852345ae127d1fa3092b95168e4a88e2f8
 runc:
  Version:          1.0.3
  GitCommit:        v1.0.3-0-gf46b6ba
 docker-init:
  Version:          0.19.0
  GitCommit:        de40ad0

docker compose version 
Docker Compose version v2.5.1
```
## Jdk 11 install
```bash
sudo add-apt-repository ppa:openjdk-r/ppa
sudo apt-get update
sudo apt install openjdk-11-jdk
```

## To Build jar
```bash
First install mvn
sudo apt update
sudo apt install default-jdk
sudo apt install maven
mvn clean package -DskipTests=true
```

## Build and Run docker images
```bash
cd docker
docker compose up -d
```

### View running docker containers
```bash
docker ps
CONTAINER ID   IMAGE                       COMMAND                  CREATED         STATUS                   PORTS                                               NAMES
f32ec6d0a550   emp                         "java -jar /opt/app/…"   6 minutes ago   Up 6 minutes             0.0.0.0:8082->8080/tcp                              emp
ea641f047a4f   adminer:4.8.1               "entrypoint.sh php -…"   6 minutes ago   Up 6 minutes             0.0.0.0:8081->8080/tcp                              adminer
42c851937e42   mysql/mysql-server:8.0.32   "/entrypoint.sh --de…"   6 minutes ago   Up 6 minutes (healthy)   3307/tcp, 33060-33061/tcp, 0.0.0.0:3307->3306/tcp   mysqldb
```


employee app `emp` is listening on 0.0.0.0:8082
```bash
curl 0.0.0.0:8082
```


## To Build and Run locally

- Make sure mysql is up

## To Build jar
```bash
mvn clean package -DskipTests=true
```

- Check the property `spring.datasource.url` in 'target/classes/application.properties' to be pointing to the correct mysql server
- To update , make the change in `src/main/resources/application.properties` and build again

## To run jar
```bash
java -jar target/*.jar
```
