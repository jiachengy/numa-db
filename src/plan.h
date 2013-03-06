#ifndef PLAN_H_
#define PLAN_H_

#include <vector>


/*
 * A query plan is a tree of PTables
 * the entries in the table directory should be
 * filled in the plan generation phase
 * 
 * Now, we manually compile the plan
 */

class Plan
{
 public:
	size_t nworkers;
	std::vector<PTable*> leaves;

};

#endif // PLAN_H_
