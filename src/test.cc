#include <iostream>
#include "numadb.h"

#include "builder.h"

using namespace std;

int main(int argc, char *argv[])
{
	TableBuilder builder;
	PTable *table = builder.Build(64,1000);

	// StorageEngine *se;

	// PTable *table = new PTable;

	// int npartitions = 16;
	// for (unsigned int i = 0; i < npartitions; i++) {
	// 	Partition *p = GeneratePartition();
	// 	table->AddPartititon(p);
	// }

	// Optimizer opt;
	// Plan plan = opt.compile();

	// Environment env;
	// QueryEngine *qe;
	// qe->query(plan, env);

}
