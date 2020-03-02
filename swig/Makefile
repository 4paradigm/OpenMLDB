.PHONY: all
all:

	swig -c++ -python -I../src/ interclient.i
	g++ -g -shared client.cc interclient_wrap.cxx ../src/client/tablet_client_wrapper.cc ../src/proto/tablet.pb.cc ../src/proto/common.pb.cc ../src/proto/type.pb.cc ../src/proto/name_server.pb.cc ../src/zk/zk_client.cc ../src/zk/dist_lock.cc ../src/client/tablet_client.cc ../src/client/ns_client.cc -std=c++11 -fPIC ../src/flags.cc -I ../src -I ../thirdparty/include -I /opt/rh/rh-python36/root/usr/include/python3.6m -L../thirdparty/lib -L ../build/lib -lzookeeper_mt -lsnappy -o _interclient.so -lbrpc -lprotobuf -lgflags -lrt -lssl -lcrypto -ldl -lleveldb -lz -lcommon
clean:
	rm -f interclient.py interclient_wrap.cxx
