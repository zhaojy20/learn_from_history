/*-------------------------------------------------------------------------
 *
 * LFH.h
 *	  learn from history.
 *
 *
 * IDENTIFICATION
 *	  src\include\optimizer\LFH.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LFH_H
#define LFH_H

#include "postgres.h"
#include "executor/execdesc.h"
#include "optimizer/pathnode.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"

typedef struct myListCell myListCell;

typedef struct myList {
	NodeTag type;
	int	length;
	myListCell* head;
	myListCell* tail;
}myList;

struct myListCell {
	void* data;
	myListCell* next;
};
typedef struct myVar {
	Expr xpr;
	unsigned int relid;
	AttrNumber	varattno;
	Oid			vartype;
	int32		vartypmod;
	Oid			varcollid;
	Index		varlevelsup;
	Index		varnoold;
	AttrNumber	varoattno;
	int			location;
}myVar;
typedef struct myConst {
	Expr xpr;
	Oid consttype;
	int32 consttypmod;
	Oid constcollid;
	int constlen;
	Datum constvalue;
	bool constisnull;
	bool constbyval;
	int location;
}myConst;
/* RestrictClause
*  any condition after WHERE.
*/
typedef struct RestrictClause {
	NodeTag type;
	NodeTag rc_type;
	Expr* left;
	Expr* right;
}RestrictClause;
/* SubRel
*  Present for Base Tables maybe. Need more consideration.
*/
typedef struct myRelInfo {
	NodeTag type;
	unsigned int relid;
	myList* RestrictClauseList;//list of baserestrict clause(RestrictClause*), like "A.age > 15 AND A.salary > 1500".
	myList* childRelList;//table previous join
	myList* joininfo;
}myRelInfo;
typedef struct History {
	NodeTag type;
	myRelInfo* content;
	Selectivity selec;
}History;
bool my_equal(void* a, void* b);
void* my_copyVar(PlannerInfo* root, const Var* from);
void* my_copyConst(PlannerInfo* root, const Const* from);
void initial_myRelInfoArray(PlannerInfo* root);
bool learn_from_history(PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outter_rel, RelOptInfo* inner_rel, List* joininfo, Selectivity* selec);
int learnSelectivity(QueryDesc* queryDesc, PlanState* planstate);
#endif