all: demo

demo: quickstart.cxx
	g++ -o demo quickstart.cxx -lstdc++ -std=c++17 -I../../openmldb/include  -L. -lopenmldbsdk -lpthread -lm -ldl -lstdc++fs

clean:
	rm -f demo
