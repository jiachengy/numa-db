CC=g++-4.7
CFLAGS=-std=c++11 -pthread -Wall
LIBS=-lpthread -lnuma -lglog

all : *.cc
	$(CC) -o test $(CFLAGS) *.cc $(LIBS)


table : util.cc builder.cc recycler.cc main.cc table.cc
	$(CC) -o test $(CFLAGS) recycler.cc util.cc builder.cc main.cc table.cc $(LIBS)