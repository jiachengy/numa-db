CC=g++
CFLAGS=-pthread -g -Wall
LIBS=-lnuma -lglog -lgflags

all: *.h *.cc
	$(CC) -o test.out $(CFLAGS) *.cc $(LIBS)
