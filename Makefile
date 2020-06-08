.PHONY: all
docker_tag := $(shell export LC_CTYPE=C && LANG=C cat /dev/urandom | tr -cd 'a-f0-9' | head -c 32) 
REF = $(shell echo ${CI_COMMIT_REF_NAME} | tr '[A-Z]' '[a-z]')
module = rtidb
tag = "pipe-$(shell echo ${CI_PIPELINE_IID} | sed 's/(//g' | sed 's/)//g')-commit-${CI_COMMIT_SHA:0:8}"
 
tablet: 
	rm -rfv release/tablet.meta.tar.gz
	rm -rfv release/tablet.tar
	cd docker && tar -zcvf ../release/tablet.meta.tar.gz META-INFO/meta META-INFO/k8s/tablet.yaml
	cd docker/tablet && docker build -t tablet:${docker_tag} . && docker save tablet:${docker_tag} > ../../release/tablet.tar && docker rmi tablet:${docker_tag}

nameserver: 
	rm -rfv release/nameserver.meta.tar.gz
	rm -rfv release/nameserver.tar
	cd docker && tar -zcvf ../release/nameserver.meta.tar.gz META-INFO/meta META-INFO/k8s/nameserver.yaml
	cd docker/nameserver && docker build -t nameserver:${docker_tag}  . && docker save nameserver:${docker_tag} > ../../release/nameserver.tar && docker rmi nameserver:${docker_tag}
blob_proxy: 
	rm -rfv release/blob_proxy.meta.tar.gz
	rm -rfv release/blob_proxy.tar
	bash steps/compile.sh
	wget -P /bin http://pkg-plus.4paradigm.com/software/docker/docker && chmod +x /bin/docker
	cd docker && tar -zcvf ../release/blob_proxy.meta.tar.gz META-INFO/meta META-INFO/k8s/blob_proxy.yaml
	echo ${CI_PIPELINE_IID}
	echo ${CI_COMMIT_SHA:0:8}
	echo ${CI_COMMIT_SHA}
	echo ${tag}
	cd docker/blob_proxy && docker build -t docker.4pd.io:env/${REF}/rtidb/blob_proxy:${tag}  . && docker push docker.4pd.io:env/${REF}/rtidb/blob_proxy:${tag}
