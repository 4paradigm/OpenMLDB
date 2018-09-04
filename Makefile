.PHONY: all
docker_tag := $(shell export LC_CTYPE=C && LANG=C cat /dev/urandom | tr -cd 'a-f0-9' | head -c 32) 
module = rtidb
 
tablet: 
	rm -rfv release/tablet.meta.tar.gz
	rm -rfv release/tablet.tar
	cd docker && tar -zcvf ../release/tablet.meta.tar.gz META-INFO
	cd docker/tablet && docker build -t ${docker_tag} . && docker save ${docker_tag} > ../../release/tablet.tar && docker rmi ${docker_tag}

nameserver: 
	rm -rfv release/nameserver.meta.tar.gz
	rm -rfv release/nameserver.tar
	cd docker && tar -zcvf ../release/nameserver.meta.tar.gz META-INFO
	cd docker/nameserver && docker build -t ${docker_tag} . && docker save ${docker_tag} > ../../release/nameserver.tar && docker rmi ${docker_tag}
