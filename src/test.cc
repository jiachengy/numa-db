#include <iostream>
#include "hashtable.h"

using namespace std;

int main(int argc, char *argv[])
{
	HashTable *ht = new HashTable(1000);

	for (unsigned int i = 0; i < 100; i++)
		ht->Put(i,i);

	for (unsigned int i = 0; i < 100; i++)
		cout << ht->Get(i) << endl;

	delete ht;
}
