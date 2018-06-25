.PHONY: all
 
module = rtidb
 
rtidb: 
	rm -rfv release/${module}.meta.tar.gz
	rm -rfv release/${module}.tar
	cd docker && tar zcvf ../release/${module}.meta.tar.gz META-INFO
	touch release/${module}.tar.gz
