/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * cypher_optimizer.c
 *
 *    Post-transform optimization pass for Cypher queries.
 *
 *    This module applies optimizations to the Query tree after cypher()
 *    transforms have been completed. At this point, the tree is standard
 *    PostgreSQL and can be mutated safely.
 *
 *    Current optimizations:
 *    - ORDER BY on vertices/edges: Replace _agtype_build_vertex/edge with
 *      the graphid id field for more efficient sorting, enabling index usage.
 */

#include "postgres.h"

#include "access/stratnum.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "parser/parse_oper.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

#include "optimizer/cypher_optimizer.h"
#include "utils/graphid.h"

#define MAX_CHAIN_DEPTH 20

typedef struct optimize_context
{
    int depth;  /* Track recursion depth to only optimize at top level */
} optimize_context;

/* Forward declarations */
static void optimize_query_internal(Query *query, optimize_context *context);
static void optimize_sort_clauses(Query *query, optimize_context *context);
static FuncExpr *is_entity_build_expr(Node *expr, bool *is_vertex);
static Node *extract_id_from_build_expr(FuncExpr *build_expr);
static FuncExpr *resolve_var_to_entity_build(Query *query, Var *var, bool *is_vertex,
                                              Query **deepest_subquery);
static bool chain_has_sort_clause(Query *outer_query, Var *start_var);
static int add_id_column_through_chain(Query *outer_query, Var *start_var, 
                                       Node *graphid_id_expr);

/*
 * chain_has_sort_clause
 *
 * Check if any subquery in the chain from the given Var has a sort clause.
 * If so, we shouldn't optimize at this level because that subquery will also
 * attempt to optimize, potentially causing conflicts in varattno references.
 *
 * Returns true if any subquery in the chain has a sortClause.
 */
static bool
chain_has_sort_clause(Query *outer_query, Var *start_var)
{
    RangeTblEntry *rte;
    Query *current_query;
    Var *current_var;
    int depth;
    Query *subquery;
    TargetEntry *sub_tle;
    
    current_query = outer_query;
    current_var = start_var;
    depth = 0;
    
    while (depth < MAX_CHAIN_DEPTH)
    {
        if (current_var->varno <= 0 || current_var->varno > list_length(current_query->rtable))
            return false;
            
        rte = (RangeTblEntry *) list_nth(current_query->rtable, current_var->varno - 1);
        
        if (rte->rtekind != RTE_SUBQUERY || rte->subquery == NULL)
            return false;
        
        subquery = rte->subquery;
        
        /* If this subquery has a sortClause, it will also try to optimize */
        if (subquery->sortClause != NIL)
            return true;
        
        if (current_var->varattno <= 0 || current_var->varattno > list_length(subquery->targetList))
            return false;
        
        sub_tle = (TargetEntry *) list_nth(subquery->targetList, current_var->varattno - 1);
        
        if (IsA(sub_tle->expr, Var))
        {
            current_var = (Var *) sub_tle->expr;
            current_query = subquery;
            depth++;
            continue;
        }
        
        return false;
    }
    
    return false;
}

/*
 * add_id_column_through_chain
 *
 * When we find an entity build expression multiple levels deep in subqueries,
 * we need to add the id column to EACH level so the outer query can reference it.
 *
 * This function:
 * 1. Adds the raw graphid id expression to the deepest subquery
 * 2. Adds a Var referencing that to each intermediate subquery
 * 3. Returns the varattno in the immediate subquery that the outer can reference
 *
 * Returns the new varattno in the immediate subquery, or 0 on failure.
 */
static int
add_id_column_through_chain(Query *outer_query, Var *start_var, Node *graphid_id_expr)
{
    RangeTblEntry *rte;
    Query *current_query;
    Var *current_var;
    int depth;
    int chain_len;
    Node *current_expr;
    int new_attno;
    int i;
    Query *subquery;
    TargetEntry *sub_tle;
    Query *subq;
    TargetEntry *new_tle;
    int varnos[MAX_CHAIN_DEPTH];
    Query *subqueries[MAX_CHAIN_DEPTH];
    
    current_query = outer_query;
    current_var = start_var;
    depth = 0;
    chain_len = 0;
    
    /* Build the chain from outer to the deepest subquery */
    while (depth < MAX_CHAIN_DEPTH)
    {
        if (current_var->varno <= 0 || current_var->varno > list_length(current_query->rtable))
            break;
            
        rte = (RangeTblEntry *) list_nth(current_query->rtable, current_var->varno - 1);
        
        if (rte->rtekind != RTE_SUBQUERY || rte->subquery == NULL)
            break;
        
        subquery = rte->subquery;
        varnos[chain_len] = current_var->varno;
        subqueries[chain_len] = subquery;
        chain_len++;
        
        if (current_var->varattno <= 0 || current_var->varattno > list_length(subquery->targetList))
            break;
        
        sub_tle = (TargetEntry *) list_nth(subquery->targetList, current_var->varattno - 1);
        
        if (IsA(sub_tle->expr, Var))
        {
            current_var = (Var *) sub_tle->expr;
            current_query = subquery;
            depth++;
            continue;
        }
        break;
    }
    
    if (chain_len == 0)
        return 0;
    
    /*
     * Work from the bottom up:
     * - Add the graphid_id_expr to the deepest subquery
     * - Then add Vars to each level that reference the level below
     */
    current_expr = graphid_id_expr;
    new_attno = 0;
    
    for (i = chain_len - 1; i >= 0; i--)
    {
        subq = subqueries[i];
        new_attno = list_length(subq->targetList) + 1;
        new_tle = makeTargetEntry((Expr *) current_expr, new_attno, NULL, true);
        subq->targetList = lappend(subq->targetList, new_tle);
        
        if (i > 0)
        {
            /* Create a Var for the parent level to reference this new column */
            current_expr = (Node *) makeVar(varnos[i], new_attno, GRAPHIDOID, -1, InvalidOid, 0);
        }
    }
    
    return new_attno;
}

/*
 * resolve_var_to_entity_build
 *
 * Follow a Var through subqueries to find the underlying entity build expression.
 * Cypher queries create nested subqueries, so ORDER BY on a vertex may require
 * traversing multiple levels to find the actual _agtype_build_vertex call.
 *
 * Returns the FuncExpr if found, NULL otherwise.
 * Sets *is_vertex to indicate vertex vs edge.
 * Sets *deepest_subquery to the Query containing the FuncExpr.
 */
static FuncExpr *
resolve_var_to_entity_build(Query *query, Var *var, bool *is_vertex,
                            Query **deepest_subquery)
{
    RangeTblEntry *rte;
    TargetEntry *sub_tle;
    FuncExpr *result;
    int depth;
    Query *current_query;
    Var *current_var;
    Query *subquery;
    
    current_query = query;
    current_var = var;
    depth = 0;
    
    while (depth < MAX_CHAIN_DEPTH)
    {
        depth++;
        
        if (current_var->varno <= 0 || current_var->varno > list_length(current_query->rtable))
            return NULL;
            
        rte = (RangeTblEntry *) list_nth(current_query->rtable, current_var->varno - 1);
        
        if (rte->rtekind != RTE_SUBQUERY || rte->subquery == NULL)
            return NULL;
        
        subquery = rte->subquery;
        
        if (current_var->varattno <= 0 || current_var->varattno > list_length(subquery->targetList))
            return NULL;
        
        sub_tle = (TargetEntry *) list_nth(subquery->targetList, current_var->varattno - 1);
        
        /* Check if this is the entity build expression */
        result = is_entity_build_expr((Node *) sub_tle->expr, is_vertex);
        if (result != NULL)
        {
            *deepest_subquery = subquery;
            return result;
        }
        
        /* If it's another Var, continue following the chain */
        if (IsA(sub_tle->expr, Var))
        {
            current_var = (Var *) sub_tle->expr;
            current_query = subquery;
            continue;
        }
        
        return NULL;
    }
    
    return NULL;
}

/*
 * optimize_cypher_query
 *
 * Main entry point for the Cypher query optimizer.
 * Called from cypher_analyze after cypher() transforms complete.
 *
 * This function recursively processes the query and all subqueries.
 */
void
optimize_cypher_query(Query *query)
{
    optimize_context context;

    if (query == NULL)
    {
        return;
    }

    memset(&context, 0, sizeof(context));
    optimize_query_internal(query, &context);
}

/*
 * optimize_query_internal
 *
 * Recursively process a Query and its subqueries.
 * We process top-down (optimize this level first, then subqueries) to ensure
 * that when an outer query modifies a subquery chain, inner queries see the
 * already-modified state. This prevents issues with chained WITH/ORDER BY.
 */
static void
optimize_query_internal(Query *query, optimize_context *context)
{
    ListCell *lc;

    if (query == NULL)
    {
        return;
    }

    /* Apply optimizations to this query level FIRST (top-down) */
    optimize_sort_clauses(query, context);

    /* Then process subqueries in RTEs */
    foreach(lc, query->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
        if (rte->rtekind == RTE_SUBQUERY && rte->subquery != NULL)
        {
            context->depth++;
            optimize_query_internal(rte->subquery, context);
            context->depth--;
        }
    }

    /* Process CTEs */
    foreach(lc, query->cteList)
    {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
        if (cte->ctequery != NULL && IsA(cte->ctequery, Query))
        {
            context->depth++;
            optimize_query_internal((Query *) cte->ctequery, context);
            context->depth--;
        }
    }
}

/*
 * optimize_sort_clauses
 *
 * Optimize ORDER BY clauses that sort by vertices or edges.
 *
 * For expressions like _agtype_build_vertex(id, label, props), we can
 * replace the sort key with just the raw graphid id field. This allows
 * PostgreSQL to use native graphid comparison (via graphid_ops) and
 * index scans on the id column instead of computing the full vertex/edge
 * for comparison.
 */
static void
optimize_sort_clauses(Query *query, optimize_context *context)
{
    ListCell *lc;
    Index max_sortgroupref = 0;
    SortGroupClause *sgc;
    TargetEntry *tle;
    FuncExpr *build_expr;
    bool is_vertex;
    Node *id_expr;
    TargetEntry *new_tle;
    bool found_in_subquery;
    int subquery_new_attno;
    Var *original_var;
    Query *deepest_subquery;
    Var *var;
    Var *new_var;

    if (query->sortClause == NIL)
    {
        return;
    }

    /* Find the maximum sortgroupref already in use */
    foreach(lc, query->targetList)
    {
        tle = (TargetEntry *) lfirst(lc);
        if (tle->ressortgroupref > max_sortgroupref)
            max_sortgroupref = tle->ressortgroupref;
    }

    /* Process each sort clause */
    foreach(lc, query->sortClause)
    {
        sgc = (SortGroupClause *) lfirst(lc);
        build_expr = NULL;
        found_in_subquery = false;
        subquery_new_attno = 0;
        original_var = NULL;
        deepest_subquery = NULL;

        tle = get_sortgroupref_tle(sgc->tleSortGroupRef, query->targetList);
        if (tle == NULL)
        {
            continue;
        }

        /*
         * If the expression is a Var, it might reference a subquery output
         * containing the entity build expression. Follow the chain.
         */
        if (IsA(tle->expr, Var))
        {
            var = (Var *) tle->expr;
            build_expr = resolve_var_to_entity_build(query, var, &is_vertex, &deepest_subquery);

            if (build_expr != NULL)
            {
                found_in_subquery = true;
                original_var = var;
            }
        }
        else
        {
            /* Check if this is directly a vertex or edge build expression */
            build_expr = is_entity_build_expr((Node *) tle->expr, &is_vertex);
        }

        if (build_expr == NULL)
        {
            continue;
        }

        /* Extract the id argument from _agtype_build_vertex/edge */
        id_expr = extract_id_from_build_expr(build_expr);
        if (id_expr == NULL)
        {
            continue;
        }

        /*
         * Use the raw graphid id expression directly for sorting.
         * This enables PostgreSQL to use native graphid btree comparison
         * (graphid_ops) and allows index scans on graphid columns.
         */

        if (found_in_subquery && original_var != NULL)
        {
            /*
             * Entity build is in a nested subquery. Add the id column through
             * the entire subquery chain so it can be referenced at this level.
             *
             * However, if ANY subquery in the chain also has a sortClause,
             * skip this optimization at this level. That subquery will also
             * try to optimize its own ORDER BY, and we need to let it handle
             * the chain modification to avoid conflicting varattno references.
             */
            if (chain_has_sort_clause(query, original_var))
            {
                continue;
            }

            subquery_new_attno = add_id_column_through_chain(query, original_var,
                                                            copyObject(id_expr));

            if (subquery_new_attno == 0)
            {
                continue;
            }

            /* Create a Var referencing the new column in the immediate subquery */
            new_var = makeVar(original_var->varno, subquery_new_attno, GRAPHIDOID, -1, InvalidOid, 0);

            max_sortgroupref++;
            new_tle = makeTargetEntry((Expr *) new_var, list_length(query->targetList) + 1, NULL, true);
            new_tle->ressortgroupref = max_sortgroupref;
        }
        else
        {
            /* Direct expression - add id column directly to this query */
            max_sortgroupref++;
            new_tle = makeTargetEntry((Expr *) copyObject(id_expr), list_length(query->targetList) + 1, NULL, true);
            new_tle->ressortgroupref = max_sortgroupref;
        }

        /* Add new target entry and update sort clause to reference it */
        query->targetList = lappend(query->targetList, new_tle);
        sgc->tleSortGroupRef = max_sortgroupref;

        /*
         * Update the sort operators for graphid type. The original SortGroupClause
         * had operators for agtype, but now we need graphid's operators.
         * Preserve the original sort direction (ASC/DESC, NULLS FIRST/LAST).
         */
        {
            Oid sortop, eqop;
            bool hashable;
            int16 strategy = BTLessStrategyNumber;  /* default ASC */
            Oid opfamily;
            Oid opcintype;

            /* Determine if original sort was DESC by checking strategy */
            if (OidIsValid(sgc->sortop) &&
                get_ordering_op_properties(sgc->sortop, &opfamily, &opcintype, &strategy))
            {
                /* strategy will be BTLessStrategyNumber (ASC) or BTGreaterStrategyNumber (DESC) */
            }

            if (strategy == BTGreaterStrategyNumber)
            {
                /* DESC - need GT operator */
                get_sort_group_operators(GRAPHIDOID,
                                         false, true, true,
                                         NULL, &eqop, &sortop,
                                         &hashable);
            }
            else
            {
                /* ASC (default) - need LT operator */
                get_sort_group_operators(GRAPHIDOID,
                                         true, true, false,
                                         &sortop, &eqop, NULL,
                                         &hashable);
            }
            sgc->sortop = sortop;
            sgc->eqop = eqop;
            sgc->hashable = hashable;
        }

        ereport(DEBUG1,
                (errmsg("optimized ORDER BY on %s to use graphid",
                        is_vertex ? "vertex" : "edge")));
    }
}

/*
 * is_entity_build_expr
 *
 * Check if the expression is a _agtype_build_vertex or _agtype_build_edge
 * function call, possibly wrapped in type coercion nodes.
 *
 * Returns the FuncExpr if it is, NULL otherwise.
 * Sets *is_vertex to true for vertices, false for edges.
 */
static FuncExpr *
is_entity_build_expr(Node *expr, bool *is_vertex)
{
    FuncExpr *funcexpr;
    Oid funcid;
    char *funcname;

    if (expr == NULL)
        return NULL;

    /* Strip type coercion wrappers */
    while (expr != NULL)
    {
        if (IsA(expr, RelabelType))
        {
            expr = (Node *) ((RelabelType *) expr)->arg;
            continue;
        }
        if (IsA(expr, CoerceViaIO))
        {
            expr = (Node *) ((CoerceViaIO *) expr)->arg;
            continue;
        }
        if (IsA(expr, FuncExpr))
        {
            funcexpr = (FuncExpr *) expr;
            funcid = funcexpr->funcid;
            funcname = get_func_name(funcid);

            if (funcname != NULL)
            {
                if (strcmp(funcname, "_agtype_build_vertex") == 0)
                {
                    pfree(funcname);
                    *is_vertex = true;
                    return funcexpr;
                }
                if (strcmp(funcname, "_agtype_build_edge") == 0)
                {
                    pfree(funcname);
                    *is_vertex = false;
                    return funcexpr;
                }
                pfree(funcname);
            }
        }
        break;
    }

    return NULL;
}

/*
 * extract_id_from_build_expr
 *
 * Extract the id argument (first argument) from _agtype_build_vertex/edge.
 */
static Node *
extract_id_from_build_expr(FuncExpr *build_expr)
{
    if (build_expr == NULL || list_length(build_expr->args) < 1)
    {
        return NULL;
    }

    return (Node *) linitial(build_expr->args);
}
