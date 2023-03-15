ROOT_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
GoLangVersion = 1.18.2
docker_registry = registry.hub.docker.com
docker_repo = juno
image_tag = latest

build_image_name = ${docker_registry}/${docker_repo}/juno-build:${image_tag}

source_repo = git@github.com:paypal/juno.git
source_branch = main

services = $(addprefix juno, serv clusterserv clustercfg storageserv client)

service_build_targets = $(addprefix build_, ${services})

.PHONY: clean fetchsrc build ${service_build_targets} build_services ${xoomservice_build_targets} build_xoomservices

all: build_services

fetchsrc: 
	#TODO -- 
	@git clone -b ${source_branch} ${source_repo} ${ROOT_DIR}/build/src/juno ;
	cd ${ROOT_DIR}/build/src/juno; echo `git rev-parse --short=8 HEAD 2> /dev/null` >> ${ROOT_DIR}/build/src/juno/git_revision.txt ; 
	if [ -f ${ROOT_DIR}/build/src/juno/third_party/apply_patch.sh ]; then ${ROOT_DIR}/build/src/juno/third_party/apply_patch.sh ; fi ;
	wget --no-check-certificate https://dl.google.com/go/go${GoLangVersion}.linux-amd64.tar.gz -O ${ROOT_DIR}/build/go${GoLangVersion}.linux-amd64.tar.gz ;

copysrc: 
	#TODO -- 
	mkdir -p ${ROOT_DIR}/build/src/juno && rm -rf ${ROOT_DIR}/build/src/juno/* ;
	rsync -av --exclude docker --exclude-from='.rsyncignore' .. ${ROOT_DIR}/build/src/juno ;
	cd ${ROOT_DIR}/build/src/juno; echo `git rev-parse --short=8 HEAD 2> /dev/null` >> ${ROOT_DIR}/build/src/juno/git_revision.txt ;
	if [ -f ${ROOT_DIR}/build/src/juno/third_party/apply_patch.sh ]; then ${ROOT_DIR}/build/src/juno/third_party/apply_patch.sh ; fi ;
	wget --no-check-certificate https://dl.google.com/go/go${GoLangVersion}.linux-amd64.tar.gz -O ${ROOT_DIR}/build/go${GoLangVersion}.linux-amd64.tar.gz ;

buildx:
	# TODO --
	docker buildx create --name=docker_gha_builder --use --driver=docker-container
	cd build && docker buildx build --build-arg GOLANG_VERSION=${GoLangVersion} --tag \
	  ${docker_registry}/${docker_repo}/juno-dev:${image_tag} --target juno-dev \
		--cache-to type=gha,mode=max \
  	--cache-from type=gha --output type=docker --load . 
	cd build && docker buildx build --build-arg GOLANG_VERSION=${GoLangVersion} --tag \
	${build_image_name} --target builder \
	--cache-to type=gha,mode=max \
	--cache-from type=gha --load .
	docker buildx rm docker_gha_builder

build:
	# TODO --
	cd build && docker build --build-arg GOLANG_VERSION=${GoLangVersion} --tag \
	  ${docker_registry}/${docker_repo}/juno-dev:${image_tag} --target juno-dev . 
	cd build && docker build --build-arg GOLANG_VERSION=${GoLangVersion} --tag \
	${build_image_name} --target builder . 

${service_build_targets}: build_juno%: create-cluster-build-directory
	#TODO -- check if need to build  
	cd cluster-service-build && docker build --tag  \
	  ${docker_registry}/${docker_repo}/juno$*:${image_tag} \
	  --build-arg BUILD_IMAGE=${build_image_name} \
	  --target juno$* .

build_services: ${service_build_targets}

create-cluster-build-directory: clean-cluster-service-build-dir
	mkdir -p cluster-service-build && \
	cp -r default-resources/* cluster-service-build/ && \
	cp -r service/* cluster-service-build/

clean-cluster-service-build-dir:
	rm -rf cluster-service-build

clean: clean-images clean-cluster-service-build-dir

clean-images:
	for s in ${services}; do \
	  docker rmi -f ${docker_registry}/${docker_repo}/$$s:${image_tag}; \
	done

clean-containers:
	-@docker ps -a -q | xargs docker rm

clean-all: clean-images clean-containers clean-cluster-service-build-dir