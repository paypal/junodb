FROM ubuntu:16.04 AS juno-base
USER root
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y \
	cmake \
	g++ \
	make \
	wget

ARG GOLANG_VERSION
COPY /go${GOLANG_VERSION}.linux-amd64.tar.gz /
RUN tar xzvf go${GOLANG_VERSION}.linux-amd64.tar.gz -C /usr/local/

######
FROM juno-base as juno-dev-base
USER root
COPY /src/juno/third_party/rocksdb /vendor/src/rocksdb

WORKDIR /vendor/src/rocksdb
RUN make static_lib; make INSTALL_PATH=/vendor/rocksdb install-static

WORKDIR /

######
FROM juno-base as juno-dev
USER root
COPY --from=juno-dev-base /vendor/rocksdb/include /usr/local/include
COPY --from=juno-dev-base /vendor/rocksdb/lib /usr/local/lib

######
FROM juno-dev as builder 
USER root
COPY /src/juno /juno
COPY /build.sh /build.sh

RUN /build.sh



