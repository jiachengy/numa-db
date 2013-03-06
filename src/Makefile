CC=g++
CFLAGS=-pthread -Wall -O3
LIBS=-lnuma -lglog -lgflags

all: *.h *.cc
	$(CC) -o test.out $(CFLAGS) *.cc $(LIBS)
