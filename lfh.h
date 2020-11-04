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
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "optimizer/pathnode.h"

#include "optimizer/mySelectivity.h"

typedef struct myListCell myListCell;

// To avoid palloc() appearing in this block, it's convinient to define new structure as myXXX.
// Not like postgres structure XXX use palloc() to allocate the memory, the structure myXXX use malloc().
// The reason is the memory allocated by palloc() would be free after each query executed.
// But if we want to learn from the history, some data must maintain during the whole period.
typedef struct myList
{
	NodeTag type; // The type is T_myList
	int	length; // The length of the List
	myListCell* head;
	myListCell* tail;
} myList;

struct myListCell
{
	void* data;
	myListCell* next;
};
// In order to label which base relation this variate from, we have to replace varno by relid in myVar.
/* The definition of structure Var shows below
	typedef struct Var
	{
		Expr		xpr;
		Index		varno;			index of this var's relation in the range table, or INNER_VAR/OUTER_VAR/INDEX_VAR
		AttrNumber	varattno;		attribute number of this var, or zero for all attrs ("whole-row Var")
		Oid			vartype;		pg_type OID for the type of this var
		int32		vartypmod;		pg_attribute typmod value
		Oid			varcollid;		OID of collation, or InvalidOid if none
		Index		varlevelsup;	for subquery variables referencing outer relations; 0 in a normal var, >0 means N levels up
		Index		varnoold;		original value of varno, for debugging
		AttrNumber	varoattno;		original value of varattno
		int			location;		token location, or -1 if unknown
	} Var; */
typedef struct myVar
{
	Expr xpr;
	unsigned int relid; // The absolute label for base relation. The relids of base relation are consistent after they were built.
	AttrNumber	varattno;
	Oid			vartype;
	int32		vartypmod;
	Oid			varcollid;
	Index		varlevelsup;
	Index		varnoold;
	AttrNumber	varoattno;
	int			location;
} myVar;
// The attribute "constvalue" in structure Const may be different even if the corresponding part in the SQL is same, so we need to do some adaptation in structure myConst.
/* The definition of structure Const shows below
typedef struct Const
{
Expr xpr;
Oid consttype;	 pg_type OID of the constant's datatype
int32 consttypmod;	typmod value, if any
Oid constcollid;	OID of collation, or InvalidOid if none
int constlen;		typlen of the constant's datatype
Datum constvalue;		the constant's value
bool constisnull;	whether the constant is null (if true, constvalue is undefined)
bool constbyval;		whether this datatype is passed by value. If true, then all the information is stored in the Datum. If false, then the Datum contains a pointer to the information.
int location;		token location, or -1 if unknown
} Const; */
typedef struct myConst
{
	Expr xpr;
	Oid consttype;
	int32 consttypmod;
	Oid constcollid;
	int constlen;
	Datum constvalue; // If the corresponding part in SQL is text, do hash and save the hash value; In other condition, save the original value.
	bool constisnull;
	bool constbyval;
	int location;
} myConst;
/* RestrictClause
*  Every predicates after "WHERE" clause are turned into this structure.
*/
typedef struct RestrictClause
{
	// Consider a predicate like "A operator B"
	NodeTag type; // The type is T_RestrictClause
	NodeTag rc_type; // Store operator
	Expr* left; // Store A
	Expr* right; // Store B
} RestrictClause;
/* sturct myRelInfo
*  The entity of relations, including base relation and temporary relation.
*  If the relation is a base relation, relid is not 0, childRelList and joininfo is NULL, while RestrictClauseList may not be NULL.
*  If the relation is a temporary relation, relid equal to 0, RestrictClauseList is NULL, but childRelList and joininfo is not NULL.
*/
typedef struct myRelInfo
{
	//Consider C = A join B, A and B are base relation and C is a temporary relation
	NodeTag type; // The type is T_myRelInfo
	unsigned int relid; // If the relation is a base relation, relid is not 0; If the relation is a temporary relation, relid equal to 0.
	myList* RestrictClauseList; // List of baserestrict clause(RestrictClause*), like "A.age > 15 AND A.salary > 1500".
	myList* leftList; // The table previous join, like A->B
	myList* rightList;
	myList* joininfo; // The join predicate between childRel
	double tuples;
} myRelInfo;
/* struct History
*  If a temporary relation appears first time, it will be recorded as a History when the SQL is executing.
*  If the temporary relation have already appeared in the previous query, we return the selectivity to help caculating the tuples more accurate.
*  The value of attribute "selec" will be assigned after the SQL query is executed.
*/
typedef struct History
{
	//Consider C = A join B
	NodeTag type; // The type is T_History
	myRelInfo* content; //The entity of this relation
	mySelectivity* selec; // The selectivity
	bool is_true;
} History;
/* Because we always add same base relation into different temporary relations' childRelList, making an array of base relations let us don't need to substantialize base relation each time during a SQL query */
void initial_myRelInfoArray(const PlannerInfo* root);
/* When opimizer meet a temporary relation check if we meet it before. If so return the true selectivity, if not, add the relation into a list of history */
bool learn_from_history(
	const PlannerInfo* root, const RelOptInfo* joinrel, const RelOptInfo* outter_rel, 
	const RelOptInfo* inner_rel, const List* joininfo, mySelectivity* myselec);
/* Get the true selectivity after executor */
int learnSelectivity(const QueryDesc* queryDesc, const PlanState* planstate);
bool my_equal(void* a, void* b);
void* my_copyVar(PlannerInfo* root, const Var* from);
void* my_copyConst(PlannerInfo* root, const Const* from);
void* my_copyRelInfo(PlannerInfo* root, const myRelInfo* from);
myList* ListRenew(myList* l, void* p);
void* getExpr(PlannerInfo* root, Node* expr);
void* getLeftandRight(PlannerInfo* root, RestrictClause* rc, Expr* clause);
void* getRestrictClause(PlannerInfo* root, List* info);
void* getChildList(PlannerInfo* root, RelOptInfo* rel);
myRelInfo* CreateNewRel(const PlannerInfo* root, const RelOptInfo* rel, const RelOptInfo* old_rel, const RelOptInfo* new_rel, const List* joininfo);
void* CreateNewHistory(const myRelInfo* rel);
History* LookupHistory(const myRelInfo* rel);
bool isEqualRel(const QueryDesc* queryDesc, myRelInfo* rel, const myRelInfo** left_array, const int lsize, const myRelInfo** right_array, const int rsize);
int FindComponent(const QueryDesc* queryDesc, const Plan* plan, myRelInfo** t_array);
History* FindCorHistory(const QueryDesc* queryDesc, const Plan* plan);
void check_two_level_rel(const QueryDesc* queryDesc, const History* his, const myRelInfo** left_array, const int lsize, const myRelInfo** right_array, const int rsize);
#endif