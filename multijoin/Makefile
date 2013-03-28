CC=g++
CFLAGS=-pthread -Wall -g
#INC=-I../lib/intel-pcm-2.4
LIBS=-lpthread -lglog -lpapi -lnuma # -lperf
#LDPATH=-L../lib/intel-pcm-2.4/lib

all : *.cc
	$(CC) -o test $(CFLAGS) *.cc $(INC) $(LDPATH) $(LIBS)

papi : testpapi.cc util.cc
	$(CC) -o test $(CFLAGS) util.cc testpapi.cc $(INC) $(LDPATH) $(LIBS)
