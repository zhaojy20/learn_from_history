/*-------------------------------------------------------------------------
 *
 * LFH.c
 *	  learn from history.
 *
 *
 * IDENTIFICATION
 *	  src\backend\optimizer\plan\LFH.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "optimizer/lfh.h"
#include "nodes/bitmapset.h"
#include <stdlib.h>
#include "utils/hashutils.h"

#define COMPARE_SCALAR_FIELD(fldname) do { if (a->fldname != b->fldname) return false;} while (0)
#define foreach_myList(cell, l)	for ((cell) = mylist_head(l); (cell) != NULL; (cell) = lnext(cell))

myList* CheckList = ((myList*)NULL); // The list of sturct History
static myRelInfo** myRelInfoArray; // The array for entity of base relations

bool my_equal(void* a, void* b);

static inline myListCell* mylist_head(const myList * l)
{
	return l ? l->head : NULL;
}
/* Compare two list, if same, return true */
bool _equalmyList(myList *a, myList *b)
{
	if (a == b)
		return true;
	if (a == NULL || b == NULL)
		return false;
	COMPARE_SCALAR_FIELD(length);
	int cnt = 0;
	myListCell *lc1, *lc2;
	foreach_myList(lc1, a) 
	{
		foreach_myList(lc2, b) 
		{
			if (my_equal(lc1->data, lc2->data)) 
			{
				cnt++;
				break;
			}
		}
	}
	if (cnt == a->length)
		return true;
	return false;
}
/* Compare two entity of relation, if same, return true */
bool _equalmyRelInfo(myRelInfo* a, myRelInfo* b)
{
	COMPARE_SCALAR_FIELD(relid);
	if(_equalmyList(a->RestrictClauseList, b->RestrictClauseList) 
		&& _equalmyList(a->childRelList, b->childRelList) 
		&& _equalmyList(a->joininfo, b->joininfo))
		return true;
	return false;
}
/* Compare two RestrictClause, if same, return true */
bool _equalRestrictClause(RestrictClause* a, RestrictClause* b)
{
	COMPARE_SCALAR_FIELD(rc_type);
	if(my_equal(a->left, b->left) && my_equal(a->right, b->right))
		return true;
	return false;
}
/* Compare two History, if same, return true */
bool _equalHistory(History* a, History* b)
{
	COMPARE_SCALAR_FIELD(selec);
	if(_equalmyRelInfo(a->content, b->content))
		return true;
	return false;
}
/* Compare two myVar, if same, return true */
bool _equalmyVar(myVar* a, myVar* b)
{
	COMPARE_SCALAR_FIELD(relid);
	COMPARE_SCALAR_FIELD(varattno);
	COMPARE_SCALAR_FIELD(vartype);
	COMPARE_SCALAR_FIELD(vartypmod);
	COMPARE_SCALAR_FIELD(varcollid);
	COMPARE_SCALAR_FIELD(varlevelsup);
	COMPARE_SCALAR_FIELD(varnoold);
	COMPARE_SCALAR_FIELD(varoattno);
	return true;
}
/* Compare two myConst, if same, return true */
bool _equalmyConst(myConst* a, myConst* b)
{
	COMPARE_SCALAR_FIELD(consttype);
	COMPARE_SCALAR_FIELD(consttypmod);
	COMPARE_SCALAR_FIELD(constcollid);
	COMPARE_SCALAR_FIELD(constlen);
	COMPARE_SCALAR_FIELD(constvalue);
	COMPARE_SCALAR_FIELD(constisnull);
	COMPARE_SCALAR_FIELD(constbyval);
	return true;
}
/* Comapre a and b. If same, return true. */
bool my_equal(void* a, void* b)
{
	bool retval = false;
	if (a == b)
		return true;
	if (a == NULL || b == NULL)
		return false;
	if (nodeTag(a) != nodeTag(b))
		return false;
	switch (nodeTag(a))
	{
		case T_myList:
			retval = _equalmyList(a, b);
			break;
		case T_myVar:
			retval = _equalmyVar(a, b);
			break;
		case T_myConst:
			retval = _equalmyConst(a, b);
			break;
		case T_RestrictClause:
			retval = _equalRestrictClause(a, b);
			break;
		case T_myRelInfo:
			retval = _equalmyRelInfo(a, b);
			break;
		case T_History:
			retval = _equalHistory(a, b);
			break;		
	}
	return retval;
}
/* Make a copy of Var. And transform the type from Var to myVar. */
void* my_copyVar(PlannerInfo* root, const Var *from)
{
	if (from == NULL)
		return NULL;
	Node* temp = (Node*)malloc(sizeof(myVar));
	if (temp)
	{
		temp->type = T_myVar;
	}
	myVar *newnode = (myVar*)temp;
	if (newnode)
	{
		newnode->relid = root->simple_rte_array[((Var*)from)->varno]->relid;
		newnode->varattno = ((Var*)from)->varattno;
		newnode->vartype = ((Var*)from)->vartype;
		newnode->vartypmod = ((Var*)from)->vartypmod;
		newnode->varcollid = ((Var*)from)->varcollid;
		newnode->varlevelsup = ((Var*)from)->varlevelsup;
		newnode->varnoold = ((Var*)from)->varnoold;
		newnode->varoattno = ((Var*)from)->varoattno;
		newnode->location = ((Var*)from)->location;
	}
	return newnode;
}
/* Make a copy of Const. And transform the type from Const to myConst. */
void* my_copyConst(PlannerInfo* root, const Const* from)
{
	if (from == NULL)
		return NULL;
	Node* temp = (Node*)malloc(sizeof(myConst));
	if (temp)
	{
		temp->type = T_myConst;
	}
	myConst* newnode = (myConst*)temp;
	if (newnode)
	{
		newnode->consttype = ((Const*)from)->consttype;
		newnode->consttypmod = ((Const*)from)->consttypmod;
		newnode->constcollid = ((Const*)from)->constcollid;
		newnode->constlen = ((Const*)from)->constlen;
		if (newnode->constlen != -1) {
			newnode->constvalue = ((Const*)from)->constvalue;
		}
		else {
			Pointer ptr = ((Pointer)((Const*)from)->constvalue);
			Pointer ptr1 = VARDATA_ANY(ptr);
			int n = VARSIZE_ANY_EXHDR(ptr);
			newnode->constvalue = hash_any(ptr1, n);
		}
		newnode->constisnull = ((Const*)from)->constisnull;
		newnode->constbyval = ((Const*)from)->constbyval;
		newnode->location = ((Const*)from)->location;
	}
	return newnode;
}
/* Make a copy of myRelInfo. */
void* my_copyRelInfo(PlannerInfo* root, const myRelInfo* from) {
	if (from == NULL)
		return NULL;
	Node* temp = (Node*)malloc(sizeof(myRelInfo));
	if (temp)
	{
		temp->type = T_myRelInfo;
	}
	myRelInfo* newnode = (myRelInfo*)temp;
	if (newnode)
	{
		newnode->relid = ((myRelInfo*)from)->relid;
		newnode->RestrictClauseList = ((myRelInfo*)from)->RestrictClauseList;
		newnode->childRelList = ((myRelInfo*)from)->childRelList;
		newnode->joininfo = ((myRelInfo*)from)->joininfo;
	}
	return newnode;
}
/* Append a myListCell to the tail of myList. */
myList* ListRenew(myList* l, void* p)
{
	if (l == (myList*)NULL)
	{
		myListCell* new_head;
		new_head = (myListCell*)malloc(sizeof(ListCell));
		if (new_head)
		{
			new_head->next = NULL;
		}
		l = (myList*)malloc(sizeof(myList));
		if (l)
		{
			l->type = T_myList;
			l->length = 1;
			l->head = new_head;
			l->tail = new_head;
		}
	}
	else
	{
		myListCell* new_tail;
		new_tail = (myListCell*)malloc(sizeof(ListCell));
		if (new_tail)
		{
			new_tail->next = NULL;
			l->tail->next = new_tail;
			l->tail = new_tail;
			l->length++;
		}
	}
	l->tail->data = p;
	return l;
}
/* Transform the input's type from XXX to myXXX */
void* getExpr(PlannerInfo* root, Node* expr)
{
	void* ans = NULL;
	switch (expr->type)
	{
		case T_Var:
		{
			/* Make a copy of Var. And transform the type from Var to myVar. */
			ans = my_copyVar(root, (const Var*)expr);
			break;
		}
		case T_Const:
		{
			/* Make a copy of Const. And transform the type from Const to myConst. */
			ans = my_copyConst(root, (const Const*)expr);
			break;
		}
		case T_RelabelType:
		{
			ans = getExpr(root, (Node*)((RelabelType*)expr)->arg);
			break;
		}
	}
	return (Expr*)ans;
}
/* transform predicates like "A.id = B.tid" to RestrictClause */
void* getLeftandRight(PlannerInfo* root, RestrictClause* rc, Expr* clause)
{
	switch (nodeTag(clause))
	{
		case T_OpExpr:
		{
			OpExpr* temp = (OpExpr*)clause;
			rc->left = getExpr(root, (Node*)(temp->args->head->data.ptr_value));
			rc->right = getExpr(root, (Node*)(temp->args->head->next->data.ptr_value));
		}
	}
	return;
}
/*
*  Tranverse the input list of predicates from Postgres to get the restriction or join information.
*/
void* getRestrictClause(PlannerInfo* root, List* info)
{
	myList* ans = (myList*)NULL;
	ListCell* lc;
	foreach(lc, info)
	{
			Expr* clause = (((RestrictInfo*)lfirst(lc))->clause);
			RestrictClause* rc = (RestrictClause*)malloc(sizeof(RestrictClause));
			rc->type = T_RestrictClause;
			rc->rc_type = clause->type;
			/* transform predicates like "A.id operator B.tid" to RestrictClause */
			getLeftandRight(root, rc, clause);
			ans = ListRenew(ans, rc);
	}
	return ans;
}
/* getChildList
*  Get the input relation from Postgres, and find out the child relations the input joined before.
*  Input is a RelOptInfo, rel->relids is a Bitmapset to record the child relations the rel joined before.
*/
void* getChildList(PlannerInfo* root, RelOptInfo* rel)
{
	myList* ans = (myList*)NULL;
	if (rel->relid == 0)
	{
		Bitmapset* temp_bms;
		temp_bms = bms_copy(rel->relids);
		int x = 0;
		while ((x = bms_first_member(temp_bms)) > 0)
		{
			myRelInfo* temp = myRelInfoArray[x];
			ans = ListRenew(ans, temp);
		}
	}
	return ans;
}
/* Make a entity of relation */
myRelInfo* CreateNewRel(const PlannerInfo* root, const RelOptInfo* rel, const List* joininfo)
{
	myRelInfo* ans = (myRelInfo*)malloc(sizeof(myRelInfo));
	ans->type = T_myRelInfo;
	/* 
	*  If rel is a base relation, has no child relation and joininfo.
	*/
	if (rel->relid != 0)
	{
		ans->relid = root->simple_rte_array[rel->relid]->relid;
		ans->childRelList = (myList*)NULL;
		ans->RestrictClauseList = getRestrictClause(root, rel->baserestrictinfo);
		ans->joininfo = (myList*)NULL;
	}
	/*
	*  If rel is a temporary relation, child relation and joininfo is useful. But no restriction information because obviously the filter predicate is only applied on the base relation.
	*/
	else
	{
		ans->relid = 0;
		ans->childRelList = getChildList(root, rel);
		ans->RestrictClauseList = (myList*)NULL;
		ans->joininfo = getRestrictClause(root, joininfo);
	}
	return ans;
}
/* Create a new History */
void* CreateNewHistory(const myRelInfo* rel)
{
	History* temp = (History*)malloc(sizeof(History));
	temp->type = T_History;
	temp->content = rel;
	temp->selec = -1.0;
	return temp;
}
/* Check myList CheckList find out if we meet this rel before ? */
bool LookupHistory(const myRelInfo* rel, Selectivity* selec)
{
	myListCell* lc;
	foreach_myList(lc, CheckList)
	{
		History* one_page = ((History*)lc->data);
		if (my_equal(one_page->content, rel))
		{
			*selec = one_page->selec;
			return true;
		}
	}
	return false;
}
/* initialize the array myRelInfoArray */
void initial_myRelInfoArray(const PlannerInfo* root)
{
	int n;
	n = root->simple_rel_array_size + 1;
	myRelInfoArray = (myRelInfo**)palloc0(n * sizeof(myRelInfo*));
}
/* learn_from_history 
*  Whenever caculating a cardinality of a temporary relation, the optimizer call this function.
*  The input joinrel is the temporary relation, inner_rel and outer_rel is joinrel's subtree.
*  If the joinrel we have met before this SQL query, we return the true selectivity instead of use Postgres's estimation.
*  If not we record this joinrel and get the true selectivity after executor. 
*/
bool learn_from_history(const PlannerInfo* root, const RelOptInfo* joinrel,
						const RelOptInfo* outer_rel, const RelOptInfo* inner_rel,
						const List* joininfo, Selectivity* selec)
{
	/* If a RelOptInfo's relid unequal to 0, meaning it's a base relation. 
	   And if we have not subtantialize this base relation, we make an entity for it. */
	if ((outer_rel->relid != 0) && (myRelInfoArray[outer_rel->relid] == NULL))
	{
		/* Make a entity of a base relation */
		myRelInfo* rel = CreateNewRel(root, outer_rel, joininfo);
		/* Add it to the myRelInfoArray */
		myRelInfoArray[outer_rel->relid] = rel;
	}
	if ((inner_rel->relid != 0) && (myRelInfoArray[outer_rel->relid] == NULL))
	{
		/* Make a entity of a base relation */
		myRelInfo* rel = CreateNewRel(root, inner_rel, joininfo);
		/* Add it to the myRelInfoArray */
		myRelInfoArray[inner_rel->relid] = rel;
	}
	/* Make a entity of a temporary relation */
	myRelInfo* rel = CreateNewRel(root, joinrel, joininfo);
	/* If we have met this temporary relation before ? */
	if (!LookupHistory(rel, selec))
	{
		/* If not, we add this temporary relation to the list of History */
		History* one_page = (History*)CreateNewHistory(rel);
		CheckList = ListRenew(CheckList, one_page);
		/* And return false imply that we haven't met this temporary relation before */
		return false;
	}
	/* If yes, we return true, and the true selectivity has been assigned to input Selectivity* selec in the function LookupHistory() */
	return true;
}
/* isCorrRel
*  If two temporary relations are same, they have same child relations.
*  One relation's child relations store in t_array, another's in rel->chidRelList.
*  We compare them and if they are same, return ture.
*/
bool isCorrRel(const QueryDesc* queryDesc, myRelInfo* rel, const int* t_array)
{
	int cnt = 0;
	int i = 0;
	while (t_array[i] != 0)
	{
		myListCell* lc;
		foreach_myList(lc, rel->childRelList)
		{
			if (((myRelInfo*)lc->data)->relid == t_array[i])
			{
				cnt++;
				break;
			}
		}
		i++;
	}
	if (cnt == i)
	{
		return true;
	}
	return false;
}
/* FindComponent
*  Get all the base relations this temporary relation(plan) has. 
*/
void FindComponent(QueryDesc* queryDesc, Plan* plan, int* t_array) 
{
	switch (plan->type)
	{
		case T_Scan:
		{
			Scan* scanplan = (Scan*)plan;
		}
		case T_SeqScan:
		{
			SeqScan* scanplan = (SeqScan*)plan;
			int i = 0;
			while (t_array[i] != 0)
			{
				i++;
			}
			t_array[i] = queryDesc->estate->es_range_table_array[scanplan->scanrelid - 1]->relid;
			break;
		}
		default: 
		{
			if (plan->lefttree)
				FindComponent(queryDesc, plan->lefttree, t_array);
			if (plan->righttree)
				FindComponent(queryDesc, plan->righttree, t_array);
			break;
		}
	}
}
/* FindCorHistory
*  Given a temporary relation(plan), find its corresponding History. 
*/
History* FindCorHistory(const QueryDesc* queryDesc, const Plan* plan) 
{
	int n = queryDesc->estate->es_range_table_size + 1;
	unsigned int* t_array = malloc(n * sizeof(unsigned int));
	memset(t_array, 0, n * sizeof(unsigned int));
	FindComponent(queryDesc, plan, t_array);
	myListCell* lc;
	foreach_myList(lc, CheckList)
	{
		if (isCorrRel(queryDesc, ((History*)lc->data)->content, t_array))
		{
			return ((History*)lc->data);
		}
	}
	return (History*)NULL;
}
/* After executor, we call this function to get the true selectivity for each temporary relation we stored in function learn_from_history().
*  Use recursion to transverse the PlanState to get the true cardinality.
*/
int learnSelectivity(const QueryDesc* queryDesc, const PlanState* planstate)
{
	Plan* plan = planstate->plan;
	// When we meet SeqScan, we reach the end of PlanState tree.
	if (plan->type == T_SeqScan)
		return plan->plan_rows;
	int left_tuple = -1, right_tuple = -1;
	History* his = (History*)NULL;
	// Get the true cardinality of the lefttree if exists.
	if(planstate->lefttree)
		left_tuple = learnSelectivity(queryDesc, planstate->lefttree);
	// Get the true cardinality of the righttree if exists.
	if(planstate->righttree)
		right_tuple = learnSelectivity(queryDesc, planstate->righttree);
	// The if clause is used to exclude T_Hash Node
	if((left_tuple != -1) && (right_tuple != -1))
		/* This PlanState Node represent a temporary relation
		*  And we need to find out the corresponding History
		*/
		his = FindCorHistory(queryDesc, plan);
	if(his)
		/* If we find a corresponding History get the true selectivity */
		his->selec = planstate->instrument->tuplecount / left_tuple / right_tuple;
	/* Return the true cardinality of this Node */
	return planstate->instrument->tuplecount;
}