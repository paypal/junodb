ARG BUILD_IMAGE
FROM  ${BUILD_IMAGE} as juno-build

######
FROM ubuntu:20.04 AS juno-runtime-base

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y daemontools
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -yqq curl dnsutils ed less netcat net-tools sysstat iperf3 iputils-ping tzdata
RUN groupadd -r juno --gid=1000 && useradd -r -g juno --uid=1000 juno 

######
FROM juno-runtime-base AS junoclusterserv
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y python

COPY --from=juno-build \
    /juno/git_revision.txt \
    /juno/cmd/etcdsvr/etcdctl \
    /juno/cmd/etcdsvr/etcdsvr_exe \
    /juno/cmd/etcdsvr/tool.py \
    /juno/cmd/etcdsvr/join.sh \
    /juno/cmd/etcdsvr/status.sh \
    /opt/juno/

COPY \
    /junoclusterserv/entrypoint.sh \
    /junoclusterserv/etcdsvr.py \
    /junoclusterserv/shutdown.sh \
    /opt/juno/

USER root

RUN chown -R juno:juno /opt/juno/

WORKDIR /opt/juno

USER juno 

ENV PATH /opt/juno:$PATH

ENTRYPOINT ["bash","/opt/juno/entrypoint.sh"]

CMD [ "python2","etcdsvr.py" ]

######
FROM juno-runtime-base AS junoclustercfg

COPY --from=juno-build \
    /juno/bin/clusterctl \
    /juno/bin/junocfg \
    /juno/bin/junoctl \
    /juno/cmd/etcdsvr/etcdctl \
    /opt/juno/

COPY /junoclustercfg/entrypoint.sh \
    /junoclustercfg/store.sh \
    /junoclustercfg/redist.sh \
    /junoclustercfg/status.sh \
    /junoclustercfg/shutdown.sh \
    /junoclustercfg/config.toml \
    /opt/juno/

USER root
RUN chown -R juno:juno /opt/juno/
RUN mv /opt/juno/clusterctl /opt/juno/clustermgr

WORKDIR /opt/juno

USER juno 

ENV PATH /opt/juno:$PATH

ENTRYPOINT ["bash","/opt/juno/entrypoint.sh"]

######
FROM juno-runtime-base AS junoserv


COPY --from=juno-build \
    /juno/bin/proxy \
    /juno/bin/junocli \
    /juno/bin/junoload \
    /juno/bin/junostats \
    /opt/juno/bin/

COPY \
    /junoserv/entrypoint.sh \
    /junoserv/shutdown.sh \
    /opt/juno/bin/

COPY \
    /junoserv/default.toml \
    /opt/juno/etc/

COPY \
    /junoserv/config.toml \
    /opt/juno/config/

USER root
RUN mkdir -p /opt/juno/cache
RUN chown -R juno:juno /opt/juno/

WORKDIR /opt/juno
USER juno 
ENV PATH /opt/juno/bin:$PATH

ENTRYPOINT ["bash","/opt/juno/bin/entrypoint.sh"]
CMD ["proxy", "manager", "-config=/opt/juno/etc/config.toml"]


######
FROM juno-runtime-base AS junostorageserv

COPY --from=juno-build \
    /juno/bin/storageserv \
    /juno/bin/dbscanserv \
    /juno/bin/dbcopy \
    /juno/bin/junocli \
    /juno/bin/junoload \ 
    /juno/bin/junostats \
    /opt/juno/bin/

COPY --from=juno-build \
    /juno/bin/dbscanserv \
    /opt/juno/

COPY \
    /junostorageserv/entrypoint.sh \
    /junostorageserv/shutdown.sh \
    /opt/juno/bin/

COPY \
    /junostorageserv/default.toml \
    /opt/juno/etc/

COPY \
    /junostorageserv/config.toml \
    /opt/juno/config/

USER root
RUN mkdir -p /opt/juno/cache
RUN chown -R juno:juno /opt/juno

WORKDIR /opt/juno
USER juno 
ENV PATH /opt/juno/bin:$PATH

ENTRYPOINT ["bash","/opt/juno/bin/entrypoint.sh"]
CMD [ "storageserv", "manager", "-config=/opt/juno/etc/config.toml" ]
