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
#include "utils/rel.h"
#include <stdlib.h>
#include "utils/hashutils.h"
#include "optimizer/mySelectivity.h"
#include "test.h"
#define COMPARE_SCALAR_FIELD(fldname) do { if (a->fldname != b->fldname) return false;} while (0)
#define foreach_myList(cell, l)	for ((cell) = mylist_head(l); (cell) != NULL; (cell) = lnext(cell))

myList* CheckList = ((myList*)NULL); // The list of sturct History
List* rtable;

bool my_equal(void* a, void* b);
void* getChildList(PlannerInfo * root, RelOptInfo * rel);
void check_two_level_rel(QueryDesc * queryDesc, History * his, List * left_array, List * right_array);

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
	#ifdef test
		test_equalmyList();
	#endif // test
	if (!_equalmyList(a->RestrictClauseList, b->RestrictClauseList))
		return false;
	if (!_equalmyList(a->joininfo, b->joininfo))
		return false;
	if (_equalmyList(a->leftList, b->leftList) && _equalmyList(a->rightList, b->rightList))
		return true;
	if (_equalmyList(a->leftList, b->rightList) && _equalmyList(a->rightList, b->leftList))
		return true;
	return false;
}
/* Compare two RestrictClause, if same, return true */
bool _equalRestrictClause(RestrictClause* a, RestrictClause* b)
{
	COMPARE_SCALAR_FIELD(rc_type);
	if(my_equal(a->left, b->left) && my_equal(a->right, b->right))
		return true;
	if (my_equal(a->left, b->right) && my_equal(a->right, b->left))
		return true;
	return false;
}
/* Compare two History, if same, return true */
bool _equalHistory(History* a, History* b)
{
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
		#ifdef test
			if (test_equalmyList() == false)
				ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmsg("_equalmyList wrong")));
		#endif // test
			retval = _equalmyList(a, b);
			break;
		case T_myVar:
		#ifdef test
			if (test_equalmyVar() == false)
				ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmsg("_equalmyVar wrong")));
		#endif // test
			retval = _equalmyVar(a, b);
			break;
		case T_myConst:
		#ifdef test
			if (test_equalmyConst() == false)
				ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmsg("_equalmyConst wrong")));
		#endif // test
			retval = _equalmyConst(a, b);
			break;
		case T_RestrictClause:
		#ifdef test
			if (test_equalRestrictClause() == false)
				ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmsg("_equalRestrictClause wrong")));
		#endif // test
			retval = _equalRestrictClause(a, b);
			break;
		case T_myRelInfo:
		#ifdef test
			if (test_equalmyRelInfo() == false)
				ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmsg("_equalmyRelInfo wrong")));
		#endif // test
			retval = _equalmyRelInfo(a, b);
			break;
		case T_History:
		#ifdef test
			if (test_equalHistory() == false)
				ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmsg("_equalHistory wrong")));
		#endif // test
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
void* my_copyRelInfo(PlannerInfo* root, const myRelInfo* from)
{
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
		newnode->leftList = ((myRelInfo*)from)->leftList;
		newnode->rightList = ((myRelInfo*)from)->rightList;
		newnode->joininfo = ((myRelInfo*)from)->joininfo;
		newnode->tuples = ((myRelInfo*)from)->tuples;
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
/* Make a entity of relation */
myRelInfo* CreateNewRel(const PlannerInfo* root, const RelOptInfo* rel, const RelOptInfo* old_rel, const RelOptInfo* new_rel, const List* joininfo)
{
	if (rel->rtekind == RTE_SUBQUERY || rel->rtekind == RTE_CTE)
	{
		return (myRelInfo*)NULL;
	}
	myRelInfo* ans = (myRelInfo*)malloc(sizeof(myRelInfo));
	ans->type = T_myRelInfo;
	/*
	*  If rel is a base relation, has no child relation and joininfo.
	*/
	if (rel->rtekind == RTE_RELATION)
	{
		ans->relid = root->simple_rte_array[rel->relid]->relid;
		ans->leftList = (myList*)NULL;
		ans->rightList = (myList*)NULL;
		ans->RestrictClauseList = getRestrictClause(root, rel->baserestrictinfo);
		ans->joininfo = (myList*)NULL;
		ans->tuples = rel->rows;
	}
	/*
	*  If rel is a temporary relation, child relation and joininfo is useful. But no restriction information because obviously the filter predicate is only applied on the base relation.
	*/
	else if (rel->rtekind == RTE_JOIN)
	{
		ans->relid = 0;
		ans->leftList = getChildList(root, old_rel);
		ans->rightList = getChildList(root, new_rel);
		ans->RestrictClauseList = (myList*)NULL;
		ans->joininfo = getRestrictClause(root, joininfo);
		ans->tuples = 0.0;
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
	if (rel->rtekind == RTE_JOIN)
	{
		Bitmapset* temp_bms;
		temp_bms = bms_copy(rel->relids);
		int x = 0;
		while ((x = bms_first_member(temp_bms)) > 0)
		{
			myRelInfo* temp = CreateNewRel(root, root->simple_rel_array[x], NULL, NULL, NULL);
			if (temp)
				ans = ListRenew(ans, temp);
		}
	}
	else if(rel->rtekind == RTE_RELATION)
	{
		myRelInfo* temp = CreateNewRel(root, rel, NULL, NULL, NULL);
		ans = ListRenew(ans, temp);
	}
	return ans;
}

/* Create a new History */
void* CreateNewHistory(const myRelInfo* rel)
{
	History* temp = (History*)malloc(sizeof(History));
	temp->type = T_History;
	temp->content = rel;
	temp->is_true = false;
	temp->selec = (mySelectivity*)malloc(sizeof(mySelectivity));
	temp->selec->max_selec = 1.0;
	temp->selec->selec = 0.0;
	temp->selec->min_selec = 0.0;
	return temp;
}
/* Check myList CheckList find out if we meet this rel before ? */
History* LookupHistory(const myRelInfo* rel)
{
	myListCell* lc;
	foreach_myList(lc, CheckList)
	{
		History* one_page = ((History*)lc->data);
		if (my_equal(one_page->content, rel))
		{
			return one_page;
		}
	}
	return NULL;
}

void initial_rtable(const PlannerInfo* root)
{
	for (int i = 1; i < root->simple_rel_array_size; i++)
	{
		if (root->simple_rel_array[i] == NULL)
			continue;
		myRelInfo* rel = CreateNewRel(root, root->simple_rel_array[i], NULL, NULL, NULL);
		rtableCell* rc = palloc(sizeof(rtableCell));
		rc->rel = rel;
		rc->name = root->simple_rte_array[i]->eref->aliasname;
		rtable = lappend(rtable, rc);
	}
}

/* learn_from_history
*  Whenever caculating a cardinality of a temporary relation, the optimizer call this function.
*  The input joinrel is the temporary relation, inner_rel and outer_rel is joinrel's subtree.
*  If the joinrel we have met before this SQL query, we return the true selectivity instead of use Postgres's estimation.
*  If not we record this joinrel and get the true selectivity after executor. 
*/
bool learn_from_history(const PlannerInfo* root, const RelOptInfo* joinrel,
						const RelOptInfo* outer_rel, const RelOptInfo* inner_rel,
						const List* joininfo, mySelectivity* myselec)
{
	myRelInfo* rel;
	if (outer_rel->rtekind == RTE_SUBQUERY || inner_rel->rtekind == RTE_SUBQUERY)
		return false;
	if (outer_rel->rtekind == RTE_RELATION) 
	{
		/* Make a entity of a temporary relation */
		rel = CreateNewRel(root, joinrel, inner_rel, outer_rel, joininfo);
	}
	else
	{
		rel = CreateNewRel(root, joinrel, outer_rel, inner_rel, joininfo);
	}
	/* If we have met this temporary relation before ? */
	History* his = LookupHistory(rel);
	if (his == NULL)
	{
		/* If not, we add this temporary relation to the list of History */
		History* one_page = (History*)CreateNewHistory(rel);
		CheckList = ListRenew(CheckList, one_page);
		/* And return false imply that we haven't met this temporary relation before */
		return false;
	}
	if (his->selec->selec > 0.0)
	{
		myselec->max_selec = his->selec->max_selec;
		myselec->selec = his->selec->selec;
		myselec->min_selec = his->selec->min_selec;
	}
	else
		return false;
	/* If yes, we return true, and the true selectivity has been assigned to input Selectivity* selec in the function LookupHistory() */
	return true;
}
/* isCorrRel
*  If two temporary relations are same, they have same child relations.
*  One relation's child relations store in t_array, another's in rel->chidRelList.
*  We compare them and if they are same, return ture.
*/
bool isEqualRel(const QueryDesc* queryDesc, myRelInfo* rel, const List* left_array, const List* right_array)
{
	int cnt = 0;
	int i = 0;
	myListCell* lc1;
	ListCell* lc2;
	if (left_array == NULL || right_array == NULL)
		return false;
	if ((rel->leftList == NULL) || (rel->rightList == NULL))
		return false;
	if ((rel->leftList->length != left_array->length) || (rel->rightList->length != right_array->length))
		return false;
	foreach_myList(lc1, rel->leftList)
	{
		foreach (lc2, left_array)
		{
			if (my_equal((myRelInfo*)lc1->data, (myRelInfo*)lc2->data.ptr_value))
			{
				cnt++;
				break;
			}
		}
	}
	if (left_array->length != cnt)
		return false;
	cnt = 0;
	foreach_myList(lc1, rel->rightList)
	{
		foreach(lc2, right_array)
		{
			if (my_equal((myRelInfo*)lc1->data, (myRelInfo*)lc2->data.ptr_value))
			{
				cnt++;
				break;
			}
		}
	}
	if (right_array->length == cnt)
	{
		return true;
	}
	return false;
}
/* isSupRel
*  If one temporary relations is another's superior relation, their leftList includes another's leftList, and their rightList includes another's rightList.
*  One relation's child relations store in t_array, another's in rel->chidRelList.
*  We compare them and if they are same, return ture.
*/
bool isSupRel(const QueryDesc* queryDesc, myRelInfo* rel, const int* t_array)
{
	int cnt = 0;
	int i = 0;
	while (t_array[i] != 0)
	{
		i++;
	}
	int array_cnt = i;
	myListCell* lc;
	if (rel->leftList->length + rel->rightList->length <= array_cnt)
		return false;
	foreach_myList(lc, rel->leftList)
	{
		for (int i = 0; t_array[i] != 0; i++)
		{
			if (((myRelInfo*)lc->data)->relid == t_array[i])
			{
				cnt++;
				break;
			}
		}
	}
	if ((cnt == 0) || (cnt == array_cnt))
		return false;
	foreach_myList(lc, rel->rightList)
	{
		for (int i = 0; t_array[i] != 0; i++)
		{
			if (((myRelInfo*)lc->data)->relid == t_array[i])
			{
				cnt++;
				break;
			}
		}
	}
	if (cnt == array_cnt)
	{
		return true;
	}
	return false;
}

myRelInfo* find_rtable(RangeTblEntry** rte_array, Index index)
{
	int level = rtable->length;
	RangeTblEntry* rte = rte_array[index];
	ListCell* lc = rtable->head;
	foreach(lc, rtable)
	{
		if (strcmp(((rtableCell*)lc->data.ptr_value)->name, rte->eref->aliasname) == 0)
			return ((rtableCell*)lc->data.ptr_value)->rel;
	}
	return NULL;
}

/* FindComponent
*  Get all the base relations this temporary relation(plan) has. 
*/
List* FindComponent(const QueryDesc* queryDesc, const Plan* plan, List* t_array) 
{
	int ans = 0;
	switch (plan->type)
	{
		case T_Scan:
		case T_SeqScan:
		case T_IndexScan:
		case T_CteScan:
		case T_BitmapIndexScan:
		{
			Scan* scanplan = (Scan*)plan;
			myRelInfo* rel = find_rtable(queryDesc->estate->es_range_table_array, scanplan->scanrelid - 1);
			if(rel)
				t_array = lappend(t_array, rel);
			return t_array;
		}
		default: 
		{
			if (plan->lefttree)
				t_array = FindComponent(queryDesc, plan->lefttree, t_array);
			if (plan->righttree)
				t_array = FindComponent(queryDesc, plan->righttree, t_array);
			return t_array;
		}
	}
}

/* After executor, we call this function to get the true selectivity for each temporary relation we stored in function learn_from_history().
*  Use recursion to transverse the PlanState to get the true cardinality.
*/
int learnSelectivity(const QueryDesc* queryDesc, const PlanState* planstate)
{
	Plan* plan = planstate->plan;
	// When we meet SeqScan, we reach the end of PlanState tree.
	if (plan->type == T_SeqScan)
	{
		return find_rtable(queryDesc->estate->es_range_table_array, ((SeqScan*)plan)->scanrelid - 1)->tuples;
	}
	else if (plan->type == T_BitmapHeapScan)
	{
		return learnSelectivity(queryDesc, planstate->lefttree);
	}
	else if (plan->type == T_BitmapIndexScan)
	{
		return find_rtable(queryDesc->estate->es_range_table_array, ((BitmapIndexScan*)plan)->scan.scanrelid - 1)->tuples;
	}
	else if (plan->type == T_IndexScan)
	{
		return find_rtable(queryDesc->estate->es_range_table_array, ((IndexScan*)plan)->scan.scanrelid - 1)->tuples;
	}
	else if (plan->type == T_CteScan)
	{
		return 0;
	}
	else if (plan->type == T_IndexOnlyScan)
	{
		return find_rtable(queryDesc->estate->es_range_table_array, ((IndexOnlyScan*)plan)->scan.scanrelid - 1)->tuples;
	}
	int left_tuple = -1, right_tuple = -1;
	History* his = (History*)NULL;
	List* left_array = NIL;
	// Get the true cardinality of the lefttree if exists.
	if (planstate->lefttree)
	{
		if (planstate->lefttree->type == T_SubqueryScanState)
		{
			left_tuple = learnSelectivity(queryDesc, ((SubqueryScanState*)planstate->lefttree)->subplan);
			left_array = FindComponent(queryDesc, ((SubqueryScanState*)planstate->lefttree)->subplan->plan, left_array);
		}
		else
		{
			left_tuple = learnSelectivity(queryDesc, planstate->lefttree);
			left_array = FindComponent(queryDesc, planstate->lefttree->plan, left_array);
		}
	}
	List* right_array = NIL;
	// Get the true cardinality of the righttree if exists.
	if (planstate->righttree)
	{
		if (planstate->righttree->type == T_SubqueryScanState)
		{
			right_tuple = learnSelectivity(queryDesc, ((SubqueryScanState*)planstate->righttree)->subplan);
			right_array = FindComponent(queryDesc, ((SubqueryScanState*)planstate->righttree)->subplan->plan, right_array);
		}
		else
		{
			right_tuple = learnSelectivity(queryDesc, planstate->righttree);
			right_array = FindComponent(queryDesc, planstate->righttree->plan, right_array);
		}
	}
	// The if clause is used to exclude T_Hash Node
	if ((left_tuple > 0) && (right_tuple > 0))
	{
		/* This PlanState Node represent a temporary relation
		*  And we need to find out the corresponding History
		*/
		myListCell *lc;
		foreach_myList(lc, CheckList)
		{
			if (isEqualRel(queryDesc, ((History*)lc->data)->content, left_array, right_array) || isEqualRel(queryDesc, ((History*)lc->data)->content, right_array, left_array))
			{
				/* If we find a corresponding History get the true selectivity */
				his = ((History*)lc->data);
				his->selec->max_selec = planstate->instrument->tuplecount / left_tuple / right_tuple;
				his->selec->selec = his->selec->max_selec;
				his->selec->min_selec = his->selec->selec;
				his->is_true = true;
			}
		}
	}
	if (his)
	{
		//由高级表推断2级表的selectivity
		check_two_level_rel(queryDesc, his, left_array, right_array);
	}
	/* Return the true cardinality of this Node */
	//For hash, we don't want to return the true rows, instead we want a estinated value, this is because we treat hash as a shell of a base relation.
	if (plan->type == T_Hash)
		return left_tuple;
	return planstate->instrument->tuplecount;
}
void check_two_level_rel(
	QueryDesc* queryDesc, History* his, 
	List* left_array, List* right_array)
{
	if (left_array->length + right_array->length > 2)
	{
		ListCell* lc1;
		ListCell* lc2;
		foreach (lc1, left_array)
		{
			foreach (lc2, right_array)
			{
				myListCell* lc;
				History* temp = (History*)NULL;
				foreach_myList(lc, CheckList)
				{
					List* ll = NIL;
					List* lr = NIL;
					ll = lappend(ll, lc1->data.ptr_value);
					lr = lappend(lr, lc2->data.ptr_value);
					//存在这样一个符合推断条件的2级表temp
					if((((History*)lc->data)->is_true == false) && 
						((isEqualRel(queryDesc, ((History*)lc->data)->content, ll, lr)
						|| (isEqualRel(queryDesc, ((History*)lc->data)->content, lr, ll)))))
					{
						temp = ((History*)lc->data);
						if (temp->selec->selec == 0.0)
						{
							temp->selec->max_selec = 1.0;
							temp->selec->selec = his->selec->selec;
							temp->selec->min_selec = 0.0;
						}
						else
						{
							temp->selec->selec = his->selec->selec;
						}
						break;
					}
				}
			}
		}
	}
}

void free_rtable()
{
	rtable = NIL;
}