#include <iostream>
#include <glog/logging.h> // logging
#include <gflags/gflags.h> // logging


using namespace std;

int main(int argc, char *argv[])
{
	google::InitGoogleLogging(argv[0]);
	FLAGS_logtostderr = true;


}
