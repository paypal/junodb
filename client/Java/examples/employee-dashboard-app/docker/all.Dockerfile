# FROM eclipse-temurin:11-jdk-alpine as builder
FROM openjdk:11 as builder

WORKDIR /opt/app
ARG LOCAL_JAVA_HOME
# COPY $LOCAL_JAVA_HOME/lib/security/cacerts $JAVA_HOME/lib/security/cacerts
COPY docker/cacerts $JAVA_HOME/lib/security/cacerts
COPY .mvn/ .mvn
COPY mvnw pom.xml ./
RUN ./mvnw dependency:go-offline
COPY src ./src
RUN ./mvnw clean package -DskipTests

FROM eclipse-temurin:11-jdk-alpine
WORKDIR /opt/app
EXPOSE 8080
COPY --from=builder /opt/app/target/*.jar /opt/app/*.jar
ENTRYPOINT ["java","-jar","/opt/app/*.jar"]