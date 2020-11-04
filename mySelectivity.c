#include "optimizer/mySelectivity.h"

typedef struct RangeQueryClause
{
	struct RangeQueryClause* next;
	Node* var;
	bool have_lobound;
	bool have_hibound;
	Selectivity lobound;
	Selectivity hibound;
} RangeQueryClause;

bool mystatext_is_compatible_clause(PlannerInfo* root, Node* clause, Index relid, Bitmapset** attnums);
Selectivity join_selectivity(PlannerInfo* root, Oid operatorid, List* args, Oid inputcollid, JoinType jointype, SpecialJoinInfo* sjinfo);
Selectivity restriction_selectivity(PlannerInfo* root, Oid operatorid, List* args, Oid inputcollid, int varRelid);

static bool bms_is_subset_singleton(const Bitmapset* s, int x)
{
	switch (bms_membership(s))
	{
		case BMS_EMPTY_SET:
			return true;
		case BMS_SINGLETON:
			return bms_is_member(x, s);
		case BMS_MULTIPLE:
			return false;
	}
	return false;
}

static inline bool mytreat_as_join_clause(Node* clause, RestrictInfo* rinfo, int varRelid, SpecialJoinInfo* sjinfo)
{
	if (varRelid != 0)
	{
		return false;
	}
	else if (sjinfo == NULL)
	{
		return false;
	}
	else
	{
		if (rinfo)
			return (bms_membership(rinfo->clause_relids) == BMS_MULTIPLE);
		else
			return (NumRelids(clause) > 1);
	}
}
static bool mystatext_is_compatible_clause_internal(PlannerInfo* root, Node* clause, Index relid, Bitmapset** attnums)
{
	if (IsA(clause, RelabelType))
		clause = (Node*)((RelabelType*)clause)->arg;
	if (IsA(clause, Var))
	{
		Var* var = (Var*)clause;
		if (var->varno != relid)
			return false;
		if (var->varlevelsup > 0)
			return false;
		if (!AttrNumberIsForUserDefinedAttr(var->varattno))
			return false;
		*attnums = bms_add_member(*attnums, var->varattno);
		return true;
	}
	if (is_opclause(clause))
	{
		RangeTblEntry* rte = root->simple_rte_array[relid];
		OpExpr* expr = (OpExpr*)clause;
		Var* var;
		if (list_length(expr->args) != 2)
			return false;
		if (!examine_opclause_expression(expr, &var, NULL, NULL))
			return false;
		if (rte->securityQuals != NIL && !get_func_leakproof(get_opcode(expr->opno)))
			return false;
		return mystatext_is_compatible_clause_internal(root, (Node*)var, relid, attnums);
	}
	if (is_andclause(clause) || is_orclause(clause) || is_notclause(clause))
	{
		BoolExpr* expr = (BoolExpr*)clause;
		ListCell* lc;
		foreach(lc, expr->args)
		{
			if (!mystatext_is_compatible_clause_internal(root, (Node*)lfirst(lc), relid, attnums))
				return false;
		}
		return true;
	}
	if (IsA(clause, NullTest))
	{
		NullTest* nt = (NullTest*)clause;
		if (!IsA(nt->arg, Var))
			return false;
		return mystatext_is_compatible_clause_internal(root, (Node*)(nt->arg), relid, attnums);
	}
	return false;
}
static bool* mcv_get_match_bitmap(PlannerInfo* root, List* clauses, Bitmapset* keys, MCVList* mcvlist, bool is_or)
{
	int i;
	ListCell* l;
	bool* matches;
	Assert(clauses != NIL);
	Assert(list_length(clauses) >= 1);
	Assert(mcvlist != NULL);
	Assert(mcvlist->nitems > 0);
	Assert(mcvlist->nitems <= STATS_MCVLIST_MAX_ITEMS);
	matches = palloc(sizeof(bool) * mcvlist->nitems);
	memset(matches, (is_or) ? false : true, sizeof(bool) * mcvlist->nitems);
	foreach(l, clauses)
	{
		Node* clause = (Node*)lfirst(l);
		if (IsA(clause, RestrictInfo))
			clause = (Node*)((RestrictInfo*)clause)->clause;
		if (is_opclause(clause))
		{
			OpExpr* expr = (OpExpr*)clause;
			FmgrInfo	opproc;
			Var* var;
			Const* cst;
			bool		varonleft;
			fmgr_info(get_opcode(expr->opno), &opproc);
			if (examine_opclause_expression(expr, &var, &cst, &varonleft))
			{
				int idx;
				idx = bms_member_index(keys, var->varattno);
				for (i = 0; i < mcvlist->nitems; i++)
				{
					bool match = true;
					MCVItem* item = &mcvlist->items[i];
					if (item->isnull[idx] || cst->constisnull)
					{
						matches[i] = RESULT_MERGE(matches[i], is_or, false);
						continue;
					}
					if (RESULT_IS_FINAL(matches[i], is_or))
						continue;
					if (varonleft)
						match = DatumGetBool(FunctionCall2Coll(&opproc, var->varcollid, item->values[idx], cst->constvalue));
					else
						match = DatumGetBool(FunctionCall2Coll(&opproc, var->varcollid, cst->constvalue, item->values[idx]));
					matches[i] = RESULT_MERGE(matches[i], is_or, match);
				}
			}
		}
		else if (IsA(clause, NullTest))
		{
			NullTest* expr = (NullTest*)clause;
			Var* var = (Var*)(expr->arg);
			int idx = bms_member_index(keys, var->varattno);
			for (i = 0; i < mcvlist->nitems; i++)
			{
				bool match = false;
				MCVItem* item = &mcvlist->items[i];
				switch (expr->nulltesttype)
				{
					case IS_NULL:
						match = (item->isnull[idx]) ? true : match;
						break;
					case IS_NOT_NULL:
						match = (!item->isnull[idx]) ? true : match;
						break;
				}
				matches[i] = RESULT_MERGE(matches[i], is_or, match);
			}
		}
		else if (is_orclause(clause) || is_andclause(clause))
		{
			int i;
			BoolExpr* bool_clause = ((BoolExpr*)clause);
			List* bool_clauses = bool_clause->args;
			bool* bool_matches = NULL;
			Assert(bool_clauses != NIL);
			Assert(list_length(bool_clauses) >= 2);
			bool_matches = mcv_get_match_bitmap(root, bool_clauses, keys, mcvlist, is_orclause(clause));
			for (i = 0; i < mcvlist->nitems; i++)
				matches[i] = RESULT_MERGE(matches[i], is_or, bool_matches[i]);

			pfree(bool_matches);
		}
		else if (is_notclause(clause))
		{
			int i;
			BoolExpr* not_clause = ((BoolExpr*)clause);
			List* not_args = not_clause->args;
			bool* not_matches = NULL;
			Assert(not_args != NIL);
			Assert(list_length(not_args) == 1);
			not_matches = mcv_get_match_bitmap(root, not_args, keys, mcvlist, false);
			for (i = 0; i < mcvlist->nitems; i++)
				matches[i] = RESULT_MERGE(matches[i], is_or, !not_matches[i]);
			pfree(not_matches);
		}
		else if (IsA(clause, Var))
		{
			Var* var = (Var*)(clause);
			int			idx = bms_member_index(keys, var->varattno);
			Assert(var->vartype == BOOLOID);
			for (i = 0; i < mcvlist->nitems; i++)
			{
				MCVItem* item = &mcvlist->items[i];
				bool		match = false;
				if (!item->isnull[idx] && DatumGetBool(item->values[idx]))
					match = true;
				matches[i] = RESULT_MERGE(matches[i], is_or, match);
			}
		}
		else
			elog(ERROR, "unknown clause type: %d", clause->type);
	}
	return matches;
}

mySelectivity* mySelec_init(Selectivity a)
{
	mySelectivity* s1 = palloc(sizeof(mySelectivity));
	s1->selec = a;
	s1->max_selec = 1.0;
	s1->min_selec = 0.0;
	return s1;
}
mySelectivity* mySelec_equal(mySelectivity* a)
{
	mySelectivity* s1 = palloc(sizeof(mySelectivity));
	s1->selec = a->selec;
	s1->max_selec = a->max_selec;
	s1->min_selec = a->min_selec;
	return s1;
}
mySelectivity* mySelec_add(mySelectivity* a, mySelectivity* b)
{
	mySelectivity* s1 = palloc(sizeof(mySelectivity));
	s1->selec = a->selec + b->selec;
	s1->max_selec = a->max_selec + b->max_selec;
	s1->min_selec = a->min_selec + b->min_selec;
	return s1;
}
mySelectivity* mySelec_sub(mySelectivity* a, mySelectivity* b)
{
	mySelectivity* s1 = palloc(sizeof(mySelectivity));
	s1->selec = a->selec - b->selec;
	s1->max_selec = a->max_selec - b->max_selec;
	s1->min_selec = a->min_selec - b->min_selec;
	return s1;
}
mySelectivity* mySelec_mul(mySelectivity* a, mySelectivity* b)
{
	mySelectivity* s1 = palloc(sizeof(mySelectivity));
	s1->selec = a->selec * b->selec;
	s1->max_selec = a->max_selec * b->max_selec;
	s1->min_selec = a->min_selec * b->min_selec;
	return s1;
}
mySelectivity* mySelec_clamp(mySelectivity* s1)
{
	if (s1->max_selec < 0.0)
		s1->max_selec = 0.0;
	else if(s1->max_selec > 1.0)
		s1->max_selec = 1.0;
	if (s1->min_selec < 0.0)
		s1->min_selec = 0.0;
	else if (s1->min_selec > 1.0)
		s1->min_selec = 1.0;
	if (s1->selec < 0.0)
		s1->selec = 0.0;
	else if (s1->selec > 1.0)
		s1->selec = 1.0;
	return s1;
}
mySelectivity* myclauselist_selectivity(
	PlannerInfo* root, List* clauses, int varRelid,
	JoinType jointype, SpecialJoinInfo* sjinfo)
{
	mySelectivity* s1 = mySelec_init(1.0);
	RelOptInfo* rel;
	Bitmapset* estimatedclauses = NULL;
	rel = myfind_single_rel_for_clauses(root, clauses);
	if (rel && rel->rtekind == RTE_RELATION && rel->statlist != NIL)
	{
		s1 = mySelec_mul(s1, mystatext_clauselist_selectivity(root, clauses, varRelid, jointype, sjinfo, rel, &estimatedclauses));
	}
	s1 = mySelec_mul(s1, myclauselist_selectivity_simple(root, clauses, varRelid, jointype, sjinfo, estimatedclauses));
	return s1;
}

mySelectivity* mystatext_mcv_clauselist_selectivity(
	PlannerInfo* root, List* clauses, int varRelid,
	JoinType jointype, SpecialJoinInfo* sjinfo,
	RelOptInfo* rel, Bitmapset** estimatedclauses)
{
	ListCell* l;
	Bitmapset** list_attnums;
	int			listidx;
	StatisticExtInfo* stat;
	List* stat_clauses;
	mySelectivity* simple_sel;
	mySelectivity* mcv_sel;
	mySelectivity* mcv_basesel;
	mySelectivity* mcv_totalsel;
	mySelectivity* other_sel;
	mySelectivity* sel;
	sel->max_selec = 1.0;
	sel->selec = 1.0;
	sel->min_selec = 0.0;
	if (!has_stats_of_kind(rel->statlist, STATS_EXT_MCV))
		return sel;
	list_attnums = (Bitmapset**)palloc(sizeof(Bitmapset*) * list_length(clauses));
	listidx = 0;
	foreach(l, clauses)
	{
		Node* clause = (Node*)lfirst(l);
		Bitmapset* attnums = NULL;
		if (!bms_is_member(listidx, *estimatedclauses) && mystatext_is_compatible_clause(root, clause, rel->relid, &attnums))
			list_attnums[listidx] = attnums;
		else
			list_attnums[listidx] = NULL;
		listidx++;
	}
	stat = choose_best_statistics(rel->statlist, STATS_EXT_MCV, list_attnums, list_length(clauses));
	if (!stat)
		return sel;
	Assert(stat->kind == STATS_EXT_MCV);
	stat_clauses = NIL;
	listidx = 0;
	foreach(l, clauses)
	{
		if (list_attnums[listidx] != NULL && bms_is_subset(list_attnums[listidx], stat->keys))
		{
			stat_clauses = lappend(stat_clauses, (Node*)lfirst(l));
			*estimatedclauses = bms_add_member(*estimatedclauses, listidx);
		}
		listidx++;
	}
	simple_sel = myclauselist_selectivity_simple(root, stat_clauses, varRelid, jointype, sjinfo, NULL);
	mcv_sel = mymcv_clauselist_selectivity(root, stat, stat_clauses, varRelid, jointype, sjinfo, rel, mcv_basesel, mcv_totalsel);
	other_sel = mySelec_sub(simple_sel, mcv_basesel);
	mySelec_clamp(other_sel);
	if (other_sel->selec > 1.0 - mcv_totalsel->selec)
		other_sel->selec = 1.0 - mcv_totalsel->selec;
	sel = mySelec_add(mcv_sel, other_sel);
	mySelec_clamp(sel);
	return sel;
}

static bool mystatext_is_compatible_clause(
	PlannerInfo* root, Node* clause, Index relid, Bitmapset** attnums)
{
	RangeTblEntry* rte = root->simple_rte_array[relid];
	RestrictInfo* rinfo = (RestrictInfo*)clause;
	Oid			userid;
	if (!IsA(rinfo, RestrictInfo))
		return false;
	/* Pseudoconstants are not really interesting here. */
	if (rinfo->pseudoconstant)
		return false;
	/* clauses referencing multiple varnos are incompatible */
	if (bms_membership(rinfo->clause_relids) != BMS_SINGLETON)
		return false;
	/* Check the clause and determine what attributes it references. */
	if (!mystatext_is_compatible_clause_internal(root, (Node*)rinfo->clause, relid, attnums))
		return false;
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
	if (pg_class_aclcheck(rte->relid, userid, ACL_SELECT) != ACLCHECK_OK)
	{
		if (bms_is_member(InvalidAttrNumber, *attnums))
		{
			/* Have a whole-row reference, must have access to all columns */
			if (pg_attribute_aclcheck_all(rte->relid, userid, ACL_SELECT, ACLMASK_ALL) != ACLCHECK_OK)
				return false;
		}
		else
		{
			int	attnum = -1;
			while ((attnum = bms_next_member(*attnums, attnum)) >= 0)
			{
				if (pg_attribute_aclcheck(rte->relid, attnum, userid, ACL_SELECT) != ACLCHECK_OK)
					return false;
			}
		}
	}
	return true;
}

RelOptInfo* myfind_single_rel_for_clauses(PlannerInfo* root, List* clauses)
{
	int lastrelid = 0;
	ListCell* l;
	foreach(l, clauses)
	{
		RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);
		int relid;
		if (!IsA(rinfo, RestrictInfo))
			return NULL;
		if (bms_is_empty(rinfo->clause_relids))
			continue;
		if (!bms_get_singleton_member(rinfo->clause_relids, &relid))
			return NULL;
		if (lastrelid == 0)
			lastrelid = relid;
		else if (relid != lastrelid)
			return NULL;
	}
	if (lastrelid != 0)
		return find_base_rel(root, lastrelid);
	return NULL;
}

mySelectivity* mystatext_clauselist_selectivity(
	PlannerInfo* root, List* clauses, int varRelid,
	JoinType jointype, SpecialJoinInfo* sjinfo,
	RelOptInfo* rel, Bitmapset** estimatedclauses)
{
	mySelectivity* sel = palloc(sizeof(mySelectivity));
	sel = mySelec_equal(mystatext_mcv_clauselist_selectivity(root, clauses, varRelid, jointype, sjinfo, rel, estimatedclauses));
	//mySelec_mul(sel, mydependencies_clauselist_selectivity(root, clauses, varRelid, jointype, sjinfo, rel, estimatedclauses));
	return sel;
}

mySelectivity* myclauselist_selectivity_simple(
	PlannerInfo* root, List* clauses, int varRelid, JoinType jointype,
	SpecialJoinInfo* sjinfo, Bitmapset* estimatedclauses)
{
	mySelectivity* s1 = mySelec_init(1.0);
	RangeQueryClause* rqlist = NULL;
	ListCell* l;
	int listidx;
	if ((list_length(clauses) == 1) && bms_num_members(estimatedclauses) == 0)
		return myclause_selectivity(root, (Node*)linitial(clauses), varRelid, jointype, sjinfo);
	listidx = -1;
	foreach(l, clauses)
	{
		Node* clause = (Node*)lfirst(l);
		RestrictInfo* rinfo;
		mySelectivity* s2 = palloc(sizeof(mySelectivity));
		listidx++;
		if (bms_is_member(listidx, estimatedclauses))
			continue;
		s2 = myclause_selectivity(root, clause, varRelid, jointype, sjinfo);
		if (IsA(clause, RestrictInfo))
		{
			rinfo = (RestrictInfo*)clause;
			if (rinfo->pseudoconstant)
			{
				s1 = mySelec_mul(s1, s2);
				continue;
			}
			clause = (Node*)rinfo->clause;
		}
		else
			rinfo = NULL;
		if (is_opclause(clause) && list_length(((OpExpr*)clause)->args) == 2)
		{
			OpExpr* expr = (OpExpr*)clause;
			bool varonleft = true;
			bool ok;
			if (rinfo)
			{
				ok = (bms_membership(rinfo->clause_relids) == BMS_SINGLETON) && (is_pseudo_constant_clause_relids(lsecond(expr->args), rinfo->right_relids) || (varonleft = false, is_pseudo_constant_clause_relids(linitial(expr->args), rinfo->left_relids)));
			}
			else
			{
				ok = (NumRelids(clause) == 1) && (is_pseudo_constant_clause(lsecond(expr->args)) || (varonleft = false, is_pseudo_constant_clause(linitial(expr->args))));
			}
			if (ok)
			{
				switch (get_oprrest(expr->opno))
				{
					case F_SCALARLTSEL:
					case F_SCALARGTSEL:
					case F_SCALARGESEL:
					default:
						s1 = mySelec_mul(s1, s2);
						break;
				}
				continue;
			}
		}
		s1 = mySelec_mul(s1, s2);
	}
	while (rqlist != NULL)
	{
		RangeQueryClause* rqnext;
		if (rqlist->have_lobound && rqlist->have_hibound)
		{
			mySelectivity* s2 = palloc(sizeof(mySelectivity));;
			if (rqlist->hibound == DEFAULT_INEQ_SEL || rqlist->lobound == DEFAULT_INEQ_SEL)
			{
				s2 = mySelec_init(DEFAULT_RANGE_INEQ_SEL);
			}
			else
			{
				s2 = mySelec_init(rqlist->hibound + rqlist->lobound - 1.0);
				s2 = mySelec_add(s2, mynulltestsel(root, IS_NULL, rqlist->var, varRelid, jointype, sjinfo));
				if (s2->selec <= 0.0)
				{
					if (s2->selec < -0.01)
					{
						s2->selec = DEFAULT_RANGE_INEQ_SEL;
					}
					else
					{
						s2->selec = 1.0e-10;
					}
				}
			}
			s1 = mySelec_mul(s1, s2);
		}
		else
		{
			if (rqlist->have_lobound)
				s1->selec *= rqlist->lobound;
			else
				s1->selec *= rqlist->hibound;
		}
		rqnext = rqlist->next;
		pfree(rqlist);
		rqlist = rqnext;
	}
	return s1;
}

mySelectivity* mymcv_clauselist_selectivity(
	PlannerInfo* root, StatisticExtInfo* stat,
	List* clauses, int varRelid, JoinType jointype,
	SpecialJoinInfo* sjinfo, RelOptInfo* rel,
	mySelectivity* basesel, mySelectivity* totalsel)
{
	int			i;
	MCVList* mcv;
	mySelectivity* s = mySelec_init(0.0);
	bool* matches = NULL;
	mcv = statext_mcv_load(stat->statOid);
	matches = mcv_get_match_bitmap(root, clauses, stat->keys, mcv, false);
	basesel->selec = 0.0;
	totalsel->selec = 0.0;
	for (i = 0; i < mcv->nitems; i++)
	{
		totalsel->selec += mcv->items[i].frequency;
		if (matches[i] != false)
		{
			basesel->selec += mcv->items[i].base_frequency;
			s->selec += mcv->items[i].frequency;
		}
	}
	return s;
}

mySelectivity* myclause_selectivity(
	PlannerInfo* root, Node* clause, int varRelid,
	JoinType jointype, SpecialJoinInfo* sjinfo)
{
	mySelectivity* s1 = mySelec_init(0.5);
	RestrictInfo* rinfo = NULL;
	if (clause == NULL)
		return s1;
	if (IsA(clause, RestrictInfo))
	{
		rinfo = (RestrictInfo*)clause;
		if (rinfo->pseudoconstant)
		{
			if (!IsA(rinfo->clause, Const))
			{
				s1->selec = 1.0;
				return s1;
			}
		}
		if (rinfo->norm_selec > 1)
		{
			s1->selec = 1.0;
			return s1;
		}
		if (varRelid == 0 || bms_is_subset_singleton(rinfo->clause_relids, varRelid))
		{
			if (jointype == JOIN_INNER)
			{
				if (rinfo->norm_selec >= 0)
				{
					s1->selec = rinfo->norm_selec;
					return s1;
				}
			}
			else
			{
				if (rinfo->outer_selec >= 0)
				{
					s1->selec = rinfo->outer_selec;
					return s1;
				}
			}
		}
		if (rinfo->orclause)
			clause = (Node*)rinfo->orclause;
		else
			clause = (Node*)rinfo->clause;
	}

	if (IsA(clause, Var))
	{
		Var* var = (Var*)clause;
		if (var->varlevelsup == 0 &&
			(varRelid == 0 || varRelid == (int)var->varno))
		{
			s1->selec = boolvarsel(root, (Node*)var, varRelid);
		}
	}
	else if (IsA(clause, Const))
	{
		Const* con = (Const*)clause;
		s1->selec = con->constisnull ? 0.0 : DatumGetBool(con->constvalue) ? 1.0 : 0.0;
	}
	else if (IsA(clause, Param))
	{
		Node* subst = estimate_expression_value(root, clause);
		if (IsA(subst, Const))
		{
			Const* con = (Const*)subst;
			s1->selec = con->constisnull ? 0.0 : DatumGetBool(con->constvalue) ? 1.0 : 0.0;
		}
	}
	else if (is_notclause(clause))
	{
		s1->selec = 1.0;
		s1 = mySelec_sub(s1, myclause_selectivity(root, (Node*)get_notclausearg((Expr*)clause), varRelid, jointype, sjinfo));
	}
	else if (is_andclause(clause))
	{
		s1->selec = myclauselist_selectivity(root, ((BoolExpr*)clause)->args, varRelid, jointype, sjinfo)->selec;
	}
	else if (is_orclause(clause))
	{
		ListCell* arg;
		s1->selec = 0.0;
		foreach(arg, ((BoolExpr*)clause)->args)
		{
			mySelectivity* s2 = myclause_selectivity(root, (Node*)lfirst(arg), varRelid, jointype, sjinfo);
			s1 = mySelec_add(s1, s2) - mySelec_mul(s1, s2);
		}
	}
	else if (is_opclause(clause) || IsA(clause, DistinctExpr))
	{
		OpExpr* opclause = (OpExpr*)clause;
		Oid opno = opclause->opno;
		if (mytreat_as_join_clause(clause, rinfo, varRelid, sjinfo))
		{
			s1 = my_eqjoinsel(root, opno, opclause->args, sjinfo);
		}
		else
		{
			s1->selec = restriction_selectivity(root, opno, opclause->args, opclause->inputcollid, varRelid);
		}
		if (IsA(clause, DistinctExpr))
			s1->selec = 1.0 - s1->selec;
	}
	else if (IsA(clause, RelabelType))
	{
		s1 = myclause_selectivity(root, (Node*)((RelabelType*)clause)->arg, varRelid, jointype, sjinfo);
	}
	else
	{
		s1->selec = boolvarsel(root, clause, varRelid);
	}
	return s1;
}

mySelectivity* mynulltestsel(
	PlannerInfo* root, NullTestType nulltesttype, Node* arg,
	int varRelid, JoinType jointype, SpecialJoinInfo* sjinfo)
{
	VariableStatData vardata;
	mySelectivity* selec = palloc(sizeof(mySelectivity));;
	examine_variable(root, arg, varRelid, &vardata);
	if (HeapTupleIsValid(vardata.statsTuple))
	{
		Form_pg_statistic stats;
		double freq_null;
		stats = (Form_pg_statistic)GETSTRUCT(vardata.statsTuple);
		freq_null = stats->stanullfrac;
		switch (nulltesttype)
		{
			case IS_NULL:
				selec = mySelec_init(freq_null);
				break;
			case IS_NOT_NULL:
				selec = mySelec_init(1.0 - freq_null);
				break;
			default:
				elog(ERROR, "unrecognized nulltesttype: %d", (int)nulltesttype);
				selec = mySelec_init(0);
				return selec;
		}
	}
	else if (vardata.var && IsA(vardata.var, Var) && ((Var*)vardata.var)->varattno < 0)
	{
		selec = (nulltesttype == IS_NULL) ? mySelec_init(0.0) : mySelec_init(1.0);
	}
	else
	{
		switch (nulltesttype)
		{
			case IS_NULL:
				selec = mySelec_init(DEFAULT_UNK_SEL);
				break;
			case IS_NOT_NULL:
				selec = mySelec_init(DEFAULT_NOT_UNK_SEL);
				break;
			default:
				elog(ERROR, "unrecognized nulltesttype: %d", (int)nulltesttype);
				selec = mySelec_init(0);
				return selec;
		}
	}
	ReleaseVariableStats(vardata);
	return mySelec_clamp(selec);
}

mySelectivity* my_eqjoinsel(PlannerInfo* root, Oid operator, List* args, SpecialJoinInfo* sjinfo)
{
	mySelectivity* selec;
	mySelectivity* selec_inner;
	VariableStatData vardata1;
	VariableStatData vardata2;
	double		nd1;
	double		nd2;
	bool		isdefault1;
	bool		isdefault2;
	Oid			opfuncoid;
	AttStatsSlot sslot1;
	AttStatsSlot sslot2;
	Form_pg_statistic stats1 = NULL;
	Form_pg_statistic stats2 = NULL;
	bool		have_mcvs1 = false;
	bool		have_mcvs2 = false;
	bool		join_is_reversed;
	RelOptInfo* inner_rel;
	get_join_variables(root, args, sjinfo, &vardata1, &vardata2, &join_is_reversed);
	nd1 = get_variable_numdistinct(&vardata1, &isdefault1);
	nd2 = get_variable_numdistinct(&vardata2, &isdefault2);
	opfuncoid = get_opcode(operator);
	memset(&sslot1, 0, sizeof(sslot1));
	memset(&sslot2, 0, sizeof(sslot2));
	if (HeapTupleIsValid(vardata1.statsTuple))
	{
		stats1 = (Form_pg_statistic)GETSTRUCT(vardata1.statsTuple);
		if (statistic_proc_security_check(&vardata1, opfuncoid))
			have_mcvs1 = get_attstatsslot(&sslot1, vardata1.statsTuple, STATISTIC_KIND_MCV, InvalidOid, ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS);
	}
	if (HeapTupleIsValid(vardata2.statsTuple))
	{
		stats2 = (Form_pg_statistic)GETSTRUCT(vardata2.statsTuple);
		if (statistic_proc_security_check(&vardata2, opfuncoid))
			have_mcvs2 = get_attstatsslot(&sslot2, vardata2.statsTuple, STATISTIC_KIND_MCV, InvalidOid, ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS);
	}
	selec_inner = my_eqjoinsel_inner(opfuncoid, &vardata1, &vardata2, nd1, nd2, isdefault1, isdefault2, &sslot1, &sslot2, stats1, stats2, have_mcvs1, have_mcvs2);
	switch (sjinfo->jointype)
	{
		case JOIN_INNER:
		case JOIN_FULL:
			selec = selec_inner;
			break;
	}
	free_attstatsslot(&sslot1);
	free_attstatsslot(&sslot2);
	ReleaseVariableStats(vardata1);
	ReleaseVariableStats(vardata2);
	mySelec_clamp(selec);
	return selec;
}
mySelectivity* my_eqjoinsel_inner(
	Oid opfuncoid, VariableStatData* vardata1, VariableStatData* vardata2,
	double nd1, double nd2, bool isdefault1, bool isdefault2,
	AttStatsSlot* sslot1, AttStatsSlot* sslot2, Form_pg_statistic stats1,
	Form_pg_statistic stats2, bool have_mcvs1, bool have_mcvs2)
{
	mySelectivity* selec = mySelec_init(0.5);
	if (have_mcvs1 && have_mcvs2)
	{
		FmgrInfo eqproc;
		bool* hasmatch1;
		bool* hasmatch2;
		double nullfrac1 = stats1->stanullfrac;
		double nullfrac2 = stats2->stanullfrac;
		double matchprodfreq, matchfreq1, matchfreq2, unmatchfreq1, unmatchfreq2, otherfreq1, otherfreq2;
		mySelectivity *totalsel1, *totalsel2;
		totalsel1 = mySelec_init(0.5);
		totalsel2 = mySelec_init(0.5);
		int i, nmatches;
		fmgr_info(opfuncoid, &eqproc);
		hasmatch1 = (bool*)palloc0(sslot1->nvalues * sizeof(bool));
		hasmatch2 = (bool*)palloc0(sslot2->nvalues * sizeof(bool));
		matchprodfreq = 0.0;
		nmatches = 0;
		for (i = 0; i < sslot1->nvalues; i++)
		{
			int j;
			for (j = 0; j < sslot2->nvalues; j++)
			{
				if (hasmatch2[j])
					continue;
				if (DatumGetBool(FunctionCall2Coll(&eqproc, sslot1->stacoll, sslot1->values[i], sslot2->values[j])))
				{
					hasmatch1[i] = hasmatch2[j] = true;
					matchprodfreq += sslot1->numbers[i] * sslot2->numbers[j];
					nmatches++;
					break;
				}
			}
		}
		CLAMP_PROBABILITY(matchprodfreq);
		matchfreq1 = unmatchfreq1 = 0.0;
		for (i = 0; i < sslot1->nvalues; i++)
		{
			if (hasmatch1[i])
				matchfreq1 += sslot1->numbers[i];
			else
				unmatchfreq1 += sslot1->numbers[i];
		}
		CLAMP_PROBABILITY(matchfreq1);
		CLAMP_PROBABILITY(unmatchfreq1);
		matchfreq2 = unmatchfreq2 = 0.0;
		for (i = 0; i < sslot2->nvalues; i++)
		{
			if (hasmatch2[i])
				matchfreq2 += sslot2->numbers[i];
			else
				unmatchfreq2 += sslot2->numbers[i];
		}
		CLAMP_PROBABILITY(matchfreq2);
		CLAMP_PROBABILITY(unmatchfreq2);
		pfree(hasmatch1);
		pfree(hasmatch2);
		otherfreq1 = 1.0 - nullfrac1 - matchfreq1 - unmatchfreq1;
		otherfreq2 = 1.0 - nullfrac2 - matchfreq2 - unmatchfreq2;
		CLAMP_PROBABILITY(otherfreq1);
		CLAMP_PROBABILITY(otherfreq2);
		totalsel1->min_selec = matchprodfreq;
		totalsel1->selec = matchprodfreq;
		totalsel1->max_selec = matchprodfreq;
		if (nd2 > sslot2->nvalues)
		{
			totalsel1->selec += unmatchfreq1 * otherfreq2 / (nd2 - sslot2->nvalues);
			totalsel1->max_selec += unmatchfreq1;
		}
		if (nd2 > nmatches)
		{
			totalsel1->selec += otherfreq1 * (otherfreq2 + unmatchfreq2) / (nd2 - nmatches);
			totalsel1->max_selec += otherfreq1;
		}
		totalsel2->min_selec = matchprodfreq;
		totalsel2->selec = matchprodfreq;
		totalsel2->max_selec = matchprodfreq;
		if (nd1 > sslot1->nvalues)
		{
			totalsel2->selec += unmatchfreq2 * otherfreq1 / (nd1 - sslot1->nvalues);
			totalsel2->max_selec += unmatchfreq2;
		}
		if (nd1 > nmatches)
		{
			totalsel2->selec += otherfreq2 * (otherfreq1 + unmatchfreq1) / (nd1 - nmatches);
			totalsel2->max_selec += otherfreq2;
		}
		selec->max_selec = (totalsel1->max_selec > totalsel2->max_selec) ? totalsel1->max_selec : totalsel2->max_selec;
		selec->selec = (totalsel1->selec < totalsel2->selec) ? totalsel1->selec : totalsel2->selec;
		selec->min_selec = (totalsel1->min_selec < totalsel2->min_selec) ? totalsel1->min_selec : totalsel2->min_selec;
	}
	else
	{
		double nullfrac1 = stats1 ? stats1->stanullfrac : 0.0;
		double nullfrac2 = stats2 ? stats2->stanullfrac : 0.0;
		selec->selec = (1.0 - nullfrac1) * (1.0 - nullfrac2);
		selec->min_selec = 0.0;
		if (nd1 > nd2)
		{
			selec->selec /= nd1;
			selec->max_selec = selec->selec * nd1 / nd2;
		}
		else
		{
			selec->selec /= nd2;
			selec->max_selec = selec->selec * nd2 / nd1;
		}
	}
	return selec;
}
