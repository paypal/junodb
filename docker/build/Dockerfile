FROM ubuntu:16.04 AS juno-base
  
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y \
	cmake \
	g++ \
	libbz2-dev \
	make \
	wget \
	zlib1g-dev 

ARG GOLANG_VERSION
COPY /go${GOLANG_VERSION}.linux-amd64.tar.gz /
RUN tar xzvf go${GOLANG_VERSION}.linux-amd64.tar.gz -C /usr/local/

######
FROM juno-base as juno-dev-base

COPY /src/juno/third_party/rocksdb /vendor/src/rocksdb
COPY /src/juno/third_party/snappy /vendor/src/snappy

WORKDIR /vendor/src/snappy
RUN cmake -DCMAKE_INSTALL_PREFIX=/vendor/snappy && make install

WORKDIR /vendor/src/rocksdb
RUN make static_lib; make INSTALL_PATH=/vendor/rocksdb install-static

WORKDIR /

######
FROM juno-base as juno-dev

COPY --from=juno-dev-base /vendor/snappy/include /usr/local/include
COPY --from=juno-dev-base /vendor/snappy/lib /usr/local/lib
COPY --from=juno-dev-base /vendor/rocksdb/include /usr/local/include
COPY --from=juno-dev-base /vendor/rocksdb/lib /usr/local/lib


######
FROM juno-dev as builder 

COPY /src/juno /juno
COPY /build.sh /build.sh

RUN /build.sh



