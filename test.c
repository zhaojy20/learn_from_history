#include "test.h"
#include "optimizer/lfh.h"
extern bool _equalmyList(myList* a, myList* b);
extern bool _equalmyVar(myVar* a, myVar* b);
extern bool _equalmyConst(myConst* a, myConst* b);
extern bool _equalRestrictClause(RestrictClause* a, RestrictClause* b);
extern bool _equalmyRelInfo(myList* a, myList* b);
bool test_equalmyList()
{
	if (_equalmyList(NULL, NULL) == false)
		return false;
	myList *l1 = malloc(sizeof(myList));
	myList *l2 = malloc(sizeof(myList));
	l1->length = 2;
	l2->length = 2;
	free(l1);
	free(l2);
	return true;
}
bool test_equalmyVar()
{
	myVar *v1 = malloc(sizeof(myVar));
	myVar *v2 = malloc(sizeof(myVar));
	v1->location = -1; v2->location = 0;
	v1->relid = 256; v2->relid = 256;
	v1->varattno = 2; v2->varattno = 2;
	v1->varcollid = 1; v2->varcollid = 1;
	v1->varlevelsup = 3; v2->varlevelsup = 3;
	v1->varnoold = 1; v1->varnoold = 1;
	v1->varoattno = 2; v1->varoattno = 2;
	v1->vartype = 34; v1->vartype = 34;
	v1->vartypmod = 123; v1->vartypmod = 123;
	Expr *a = malloc(sizeof(Expr)); Expr* b = malloc(sizeof(Expr));
	a->type = T_myVar;
	b->type = T_myVar;
	v1->xpr = *a; v2->xpr = *b;
	if (_equalmyVar(v1, v2) == false)
	{
		free(v1);
		free(v2);
		return false;
	}
	free(v1);
	free(v2);
	return true;
}
bool test_equalmyConst()
{
	myConst* c1 = malloc(sizeof(myConst));
	myConst* c2 = malloc(sizeof(myConst));
	c1->constbyval = 10; c2->constbyval = 10;
	c1->constcollid = 1; c2->constcollid = 1;
	c1->constisnull = false; c2->constisnull = false;
	c1->constlen = 10; c2->constlen = 10;
	c1->consttype = 11; c2->consttype = 11;
	c1->consttypmod = 0; c2->consttypmod = 0;
	c1->constvalue = 7; c2->constvalue = 7;
	c1->location = -1; c2->location = -1;
	Expr* a = malloc(sizeof(Expr)); Expr* b = malloc(sizeof(Expr));
	a->type = T_myConst;
	b->type = T_myConst;
	c1->xpr = *a; c2->xpr = *b;
	if (_equalmyConst(c1, c2) == false)
	{
		free(c1);
		free(c2);
		return false;
	}
	free(c1);
	free(c2);
	return true;
}
bool test_equalRestrictClause()
{
	RestrictClause* rc1 = malloc(sizeof(RestrictClause));
	RestrictClause* rc2 = malloc(sizeof(RestrictClause));
	rc1->type = T_RestrictClause; rc2->type = T_RestrictClause;
	rc1->rc_type = T_OpExpr; rc2->rc_type = T_OpExpr;
	rc1->left = NULL; rc2->left = NULL;
	rc1->right = NULL; rc2->right = NULL;
	if (_equalRestrictClause(rc1, rc2) == false)
	{
		free(rc1);
		free(rc2);
		return false;
	}
	free(rc1);
	free(rc2);
	return true;
}
bool test_equalmyRelInfo()
{
	myRelInfo *r1 = malloc(sizeof(myRelInfo));
	myRelInfo *r2 = malloc(sizeof(myRelInfo));
	r1->type = T_myRelInfo; r2->type = T_myRelInfo;
	r1->joininfo = NULL; r2->joininfo = NULL;
	r1->leftList = NULL; r2->leftList = NULL;
	r1->relid = 0; r2->relid = 0;
	r1->RestrictClauseList = NULL; r2->RestrictClauseList = NULL;
	r1->rightList = NULL; r2->rightList = NULL;
	r1->tuples = 1; r2->tuples = 5;
	if (_equalmyRelInfo(r1, r2) == false)
	{
		free(r1);
		free(r2);
		return false;
	}
	free(r1);
	free(r2);
	return true;
}
bool test_equalHistory()
{
	History* h1 = malloc(sizeof(History));
	History* h2 = malloc(sizeof(History));
	h1->type = T_History; h2->type = T_History;
	h1->content = NULL; h2->content = NULL;
	h1->is_true = true; h2->is_true = false;
	mySelectivity* s1 = malloc(sizeof(mySelectivity)); mySelectivity* s2 = malloc(sizeof(mySelectivity));
	s1->max_selec = 0.6; s1->min_selec = 0.4; s1->selec = 0.5; s2->max_selec = 1.0; s2->min_selec = 0.0; s2->selec = 1.0;
	h1->selec = s1; h2->selec = s2;
	if (_equalmyRelInfo(h1, h2) == false)
	{
		free(h1);
		free(h2);
		return false;
	}
	free(h1);
	free(h2);
	return true;
}