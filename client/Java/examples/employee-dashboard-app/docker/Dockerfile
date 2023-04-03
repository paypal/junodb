FROM openjdk:11
WORKDIR /opt/app
VOLUME /tmp
EXPOSE 9080
COPY /target/*.jar /opt/app/*.jar
ENTRYPOINT ["java","-jar","/opt/app/*.jar"]