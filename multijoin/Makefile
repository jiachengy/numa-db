USE_PERF=1

CC=g++
CFLAGS=-pthread -Wall -O3
PERF_LIB=-lpapi
DEFAULT_LIB=-lpthread -lglog -lnuma # -lperf
#INC=-I../lib/intel-pcm-2.4
#LDPATH=-L../lib/intel-pcm-2.4/lib

ifeq ($(USE_PERF), 1)
	LIBS=$(DEFAULT_LIB) $(PERF_LIB)
else
	LIBS=$(DEFAULT_LIB)
endif

all : *.cc
	$(CC) -o test $(CFLAGS) *.c *.cc $(INC) $(LDPATH) $(LIBS)

