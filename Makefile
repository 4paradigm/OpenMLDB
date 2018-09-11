.PHONY: all
#docker_tag := $(shell export LC_CTYPE=C && LANG=C cat /dev/urandom | tr -cd 'a-f0-9' | head -c 32) 
module = rtidb
version = v1.3.5
 
tablet: 
	rm -rfv release/tablet.meta.tar.gz
	rm -rfv release/tablet.tar
	cd docker && tar -zcvf ../release/tablet.meta.tar.gz META-INFO
	cd docker/tablet && docker build -t docker02:35000/tablet:${version} . && docker save docker02:35000/tablet:${version} > ../../release/tablet.tar && docker rmi docker02:35000/tablet:${version}

nameserver: 
	rm -rfv release/nameserver.meta.tar.gz
	rm -rfv release/nameserver.tar
	cd docker && tar -zcvf ../release/nameserver.meta.tar.gz META-INFO
	cd docker/nameserver && docker build -t docker02:35000/nameserver:${version}  . && docker save docker02:35000/nameserver:${version} > ../../release/nameserver.tar && docker rmi docker02:35000/nameserver:${version}
