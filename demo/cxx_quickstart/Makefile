all: demo

demo: quickstart.cxx
	g++ -o demo quickstart.cxx -lstdc++ -std=c++17 -I/work/openmldb/include  -L/work/openmldb/lib -lopenmldbsdk -lpthread -lm -ldl -lstdc++fs

clean:
	rm -f demo
