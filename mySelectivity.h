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
#ifndef MYSELECTIVITY_H
#define MYSELECTIVITY_H

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tuptoaster.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_statistic_ext_d.h"
#include "catalog/pg_statistic_ext_data.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "postmaster/autovacuum.h"
#include "statistics/extended_stats_internal.h"
#include "statistics/statistics.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"

#define RESULT_MERGE(value, is_or, match) \
	((is_or) ? ((value) || (match)) : ((value) && (match)))

#define RESULT_IS_FINAL(value, is_or)	((is_or) ? (value) : (!(value)))

typedef struct mySelectivity
{
	Selectivity selec;
	Selectivity max_selec;
	Selectivity min_selec;
}mySelectivity;

mySelectivity* mystatext_mcv_clauselist_selectivity(
	PlannerInfo* root, List* clauses, int varRelid,
	JoinType jointype, SpecialJoinInfo* sjinfo,
	RelOptInfo* rel, Bitmapset** estimatedclauses);
mySelectivity* myclauselist_selectivity(
	PlannerInfo* root, List* clauses, int varRelid,
	JoinType jointype, SpecialJoinInfo* sjinfo);
mySelectivity* mystatext_clauselist_selectivity(
	PlannerInfo* root, List* clauses, int varRelid,
	JoinType jointype, SpecialJoinInfo* sjinfo,
	RelOptInfo* rel, Bitmapset** estimatedclauses);
mySelectivity* myclauselist_selectivity_simple(
	PlannerInfo* root, List* clauses, int varRelid,
	JoinType jointype, SpecialJoinInfo* sjinfo,
	Bitmapset* estimatedclauses);
mySelectivity* myclause_selectivity(
	PlannerInfo* root, Node* clause, int varRelid,
	JoinType jointype, SpecialJoinInfo* sjinfo);
mySelectivity* mymcv_clauselist_selectivity(
	PlannerInfo* root, StatisticExtInfo* stat,
	List* clauses, int varRelid, JoinType jointype,
	SpecialJoinInfo* sjinfo, RelOptInfo* rel,
	mySelectivity* basesel, mySelectivity* totalsel);
mySelectivity* mynulltestsel(
	PlannerInfo* root, NullTestType nulltesttype, Node* arg,
	int varRelid, JoinType jointype, SpecialJoinInfo* sjinfo);
mySelectivity* my_eqjoinsel(PlannerInfo* root, Oid operator, List* args, SpecialJoinInfo* sjinfo);
RelOptInfo* myfind_single_rel_for_clauses(PlannerInfo* root, List* clauses);
mySelectivity* my_eqjoinsel_inner(
	Oid opfuncoid, VariableStatData* vardata1, VariableStatData* vardata2,
	double nd1, double nd2, bool isdefault1, bool isdefault2,
	AttStatsSlot* sslot1, AttStatsSlot* sslot2, Form_pg_statistic stats1,
	Form_pg_statistic stats2, bool have_mcvs1, bool have_mcvs2);
#endif