CC=g++
DEFINES=-DDEBUG
CFLAGS=-pthread -Wall -msse4 -g -O3
LIBS=-lpthread -lnuma -lpapi

all : *.cc
	$(CC) -o test $(DEFINES) $(CFLAGS) *.c *.cc $(LIBS)

