/* Inspect an existing child plan without moving or evaluating its expressions. */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_type_d.h"
#include "nodes/nodeFuncs.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"

#include "executor/sem_prefetch.h"

static bool semloom_safe_predicate(Node *node);

static bool
semloom_safe_value(Node *node)
{
	Oid type;
	if (node == NULL) return false;
	type = exprType(node);
	if (type != BOOLOID && type != INT2OID && type != INT4OID && type != INT8OID &&
		type != TEXTOID && type != VARCHAROID)
		return false;
	if (IsA(node, Var) || IsA(node, Const)) return true;
	if (IsA(node, Param)) return ((Param *) node)->paramkind == PARAM_EXTERN;
	if (IsA(node, RelabelType))
		return semloom_safe_value((Node *) ((RelabelType *) node)->arg);
	return false;
}

static bool
semloom_safe_operator(OpExpr *op)
{
	HeapTuple tuple;
	Oid function;
	bool builtin;
	if (op->opretset || op->opresulttype != BOOLOID || list_length(op->args) != 2)
		return false;
	if (!semloom_safe_value(linitial(op->args)) || !semloom_safe_value(lsecond(op->args)))
		return false;
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
	if (!HeapTupleIsValid(tuple)) return false;
	builtin = ((Form_pg_operator) GETSTRUCT(tuple))->oprnamespace == PG_CATALOG_NAMESPACE;
	ReleaseSysCache(tuple);
	if (!builtin) return false;
	function = get_opcode(op->opno);
	switch (function)
	{
		case F_BOOLEQ: case F_BOOLNE:
		case F_INT2EQ: case F_INT2NE: case F_INT2LT: case F_INT2LE: case F_INT2GT: case F_INT2GE:
		case F_INT4EQ: case F_INT4NE: case F_INT4LT: case F_INT4LE: case F_INT4GT: case F_INT4GE:
		case F_INT8EQ: case F_INT8NE: case F_INT8LT: case F_INT8LE: case F_INT8GT: case F_INT8GE:
			return true;
		case F_TEXTEQ: case F_TEXTNE:
			/* Deterministic equality compares the legal stored text bytes. */
			return OidIsValid(op->inputcollid) && get_collation_isdeterministic(op->inputcollid);
		default:
			return false;
	}
}

static bool
semloom_safe_predicate(Node *node)
{
	ListCell *cell;
	if (node == NULL) return true;
	if (IsA(node, List))
	{
		foreach(cell, (List *) node)
			if (!semloom_safe_predicate(lfirst(cell))) return false;
		return true;
	}
	if (IsA(node, OpExpr)) return semloom_safe_operator((OpExpr *) node);
	if (IsA(node, BoolExpr))
	{
		foreach(cell, ((BoolExpr *) node)->args)
			if (!semloom_safe_predicate(lfirst(cell))) return false;
		return true;
	}
	if (IsA(node, NullTest))
		return !((NullTest *) node)->argisrow && semloom_safe_value((Node *) ((NullTest *) node)->arg);
	if (IsA(node, BooleanTest))
		return semloom_safe_predicate((Node *) ((BooleanTest *) node)->arg);
	return exprType(node) == BOOLOID && semloom_safe_value(node);
}

const char *
semloom_prefetch_reason(Plan *plan, Node *input, bool allow_predicates)
{
	ListCell *cell;
	const char *reason;
	if (input != NULL && !IsA(input, Var) && !IsA(input, Const))
		return "input expression requires demand evaluation";
	if (plan == NULL) return NULL;
	if (IsA(plan, Limit)) return "strict demand: LIMIT/OFFSET";
	if (!IsA(plan, SeqScan) && !IsA(plan, Result))
		return "child plan is outside the prefetch whitelist";
	if (plan->initPlan != NIL)
		return "child has initialization subplans";
	foreach(cell, plan->targetlist)
	{
		Node *expression = (Node *) lfirst_node(TargetEntry, cell)->expr;
		if (!IsA(expression, Var) && !IsA(expression, Const))
			return "child projection requires demand evaluation";
	}
	if (plan->qual != NIL && !allow_predicates)
		return "predicate prefetch is disabled";
	if (!semloom_safe_predicate((Node *) plan->qual))
		return "predicate operator, type or expression is outside the whitelist";
	if (IsA(plan, Result) && !semloom_safe_predicate(((Result *) plan)->resconstantqual))
		return "one-time predicate requires demand evaluation";
	reason = semloom_prefetch_reason(plan->lefttree, NULL, allow_predicates);
	if (reason != NULL) return reason;
	return semloom_prefetch_reason(plan->righttree, NULL, allow_predicates);
}
