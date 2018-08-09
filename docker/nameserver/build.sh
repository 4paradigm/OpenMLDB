#cp ../rtidb-cluster-1.3.0/bin/rtidb .
#cp ../rtidb-cluster-1.3.0/bin/boot_ns.sh .
#cp ../rtidb-cluster-1.3.0/conf/nameserver.flags .
#imagetag=`date +%Y-%m-%d`
docker build -t docker02:35000/nameserver:v3.1.0 .
docker push docker02:35000/nameserver:v3.1.0
