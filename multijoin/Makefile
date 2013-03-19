CC=g++
CFLAGS=-pthread -Wall
LIBS=-lpthread -lnuma -lglog

all : *.cc
	$(CC) -o test $(CFLAGS) *.cc $(LIBS)


table : util.cc builder.cc recycler.cc main.cc
	$(CC) -o test $(CFLAGS) recycler.cc util.cc builder.cc main.cc $(LIBS)