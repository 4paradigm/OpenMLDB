.PHONY: all
docker_tag := $(shell export LC_CTYPE=C && LANG=C cat /dev/urandom | tr -cd 'a-f0-9' | head -c 32) 
module = rtidb
 
rtidb: 
	rm -rfv release/${module}.meta.tar.gz
	rm -rfv release/${module}.tar
	cd docker && tar -zcvf ../release/${module}.meta.tar.gz META-INFO
	touch release/${module}.tar
	cd docker && docker build -t ${docker_tag} . && docker save ${docker_tag} > ../release/${module}.tar && docker rmi ${docker_tag}
