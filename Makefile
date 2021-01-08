.PHONY: all
docker_tag := $(shell export LC_CTYPE=C && LANG=C cat /dev/urandom | tr -cd 'a-f0-9' | head -c 32) 
REF = $(shell echo ${CI_COMMIT_REF_NAME} | tr '[A-Z]' '[a-z]')
module = rtidb
commit_sha_prefix = $(shell v='${CI_COMMIT_SHA}'; echo $${v:0:8})
tag = "pipe-$(shell echo ${CI_PIPELINE_IID} | sed 's/(//g' | sed 's/)//g')-commit-${commit_sha_prefix}"
 
tablet: 
	rm -rfv release/tablet.meta.tar.gz
	rm -rfv release/tablet.tar
	cp release/bin/monitor.py docker/bin
	cd docker && tar -zcvf ../release/tablet.meta.tar.gz META-INFO/meta META-INFO/k8s/tablet.yaml
	cd docker/tablet && docker build -t tablet:${docker_tag} . && docker save tablet:${docker_tag} > ../../release/tablet.tar && docker rmi tablet:${docker_tag}

nameserver: 
	rm -rfv release/nameserver.meta.tar.gz
	rm -rfv release/nameserver.tar
	cp release/bin/monitor.py docker/bin
	cd docker && tar -zcvf ../release/nameserver.meta.tar.gz META-INFO/meta META-INFO/k8s/nameserver.yaml
	cd docker/nameserver && docker build -t nameserver:${docker_tag}  . && docker save nameserver:${docker_tag} > ../../release/nameserver.tar && docker rmi nameserver:${docker_tag}
blob_proxy: 
	rm -rfv release/blob_proxy.meta.tar.gz
	rm -rfv release/blob_proxy.tar
	cp release/bin/monitor.py docker/bin
	[ -f "${HOME}/.docker/config.json" ] || mkdir ~/.docker/ && echo ewogICJhdXRocyI6IHsKICAgICJkb2NrZXItc2VhcmNoLjRwZC5pbyI6IHsKICAgICAgImF1dGgiOiAiWkdWd2JHOTVPa2RzVnpWVFVtOHhWRU16Y1E9PSIKICAgIH0sCiAgICAiZG9ja2VyLjRwZC5pbyI6IHsKICAgICAgImF1dGgiOiAiWkdWd2JHOTVPa2RzVnpWVFVtOHhWRU16Y1E9PSIKICAgIH0sCiAgICAiZG9ja2VyMDI6MzUwMDAiOiB7CiAgICAgICJhdXRoIjogImRHVnpkSFZ6WlhJNmRHVnpkSEJoYzNOM2IzSmsiCiAgICB9LAogICAgInJlZ2lzdHJ5LjRwYXJhZGlnbS5jb20iOiB7CiAgICAgICJhdXRoIjogIlpHOWphMlZ5TFhKbFoybHpkSEo1T2pGeFlYbzViMnd1IgogICAgfQogIH0KfQo= | base64 -d > "${HOME}/.docker/config.json"
	wget -P /bin http://pkg-plus.4paradigm.com/software/docker/docker && chmod +x /bin/docker
	cd docker && tar -zcvf ../rtidb.meta.tar.gz META-INFO/meta META-INFO/k8s/blob_proxy.yaml
	cd docker && docker build -t docker.4pd.io/env/${REF}/prophet/app/rtidb.tar:${tag}  . && docker push docker.4pd.io/env/${REF}/prophet/app/rtidb.tar:${tag}
