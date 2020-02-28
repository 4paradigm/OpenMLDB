.PHONY: al
all:
	swig -c++ -python -I../src/ interclient.i
	g++ -g -shared client.cc interclient_wrap.cxx -std=c++11 -fPIC ../src/flags.cc -I ../src -I ../thirdparty/include -I /usr/include/python3.6m -L../thirdparty/lib -L ../build/lib -lrtidb_py -lzk_client -lzookeeper_mt -lclient -lnameserver -lsnappy -o _interclient.so
