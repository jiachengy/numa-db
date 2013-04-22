CC=g++
DEFINES=-DDEBUG -DUSE_PERF
DEFINES+=-DCOLUMN_WISE
#DEFINES+=-DNAIVE
#DEFINES+=-DNUMA_INTERLEAVED
CFLAGS=-pthread -Wall -msse4 -O3
LIBS=-lpthread -lnuma -lpapi

all : *.cc
	$(CC) -o test $(DEFINES) $(CFLAGS) *.c *.cc $(LIBS)

