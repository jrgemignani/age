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
 * cypher_order_by_optimize.sql
 *
 * Regression tests for ORDER BY optimization on vertices and edges.
 * The optimizer should replace _agtype_build_vertex/edge with raw graphid (p.id)
 * in sort keys for more efficient sorting using native graphid comparison.
 *
 * Each test verifies that the Sort Key in EXPLAIN output uses the raw graphid
 * instead of the full _agtype_build_vertex/edge expression.
 */

LOAD 'age';
SET search_path TO ag_catalog;

SELECT create_graph('order_by_opt');

--
-- Create test data: chain of people connected by 'knows' edges
-- A -> B -> C -> D -> E -> F -> G -> H
--
SELECT * FROM cypher('order_by_opt', $$
    CREATE (a:Person {name: 'A'})
    CREATE (b:Person {name: 'B'})
    CREATE (c:Person {name: 'C'})
    CREATE (d:Person {name: 'D'})
    CREATE (e:Person {name: 'E'})
    CREATE (f:Person {name: 'F'})
    CREATE (g:Person {name: 'G'})
    CREATE (h:Person {name: 'H'})
    CREATE (a)-[:knows]->(b)
    CREATE (b)-[:knows]->(c)
    CREATE (c)-[:knows]->(d)
    CREATE (d)-[:knows]->(e)
    CREATE (e)-[:knows]->(f)
    CREATE (f)-[:knows]->(g)
    CREATE (g)-[:knows]->(h)
$$) AS (result agtype);

--
-- Test 1: Simple MATCH with ORDER BY vertex
-- Sort Key should use raw graphid (p.id)
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (p:Person)
    RETURN p
    ORDER BY p
$$) AS (person agtype);

-- Verify optimization: Sort Key should show p.id (raw graphid)
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (p:Person)
    RETURN p
    ORDER BY p
$$) AS (plan agtype);

--
-- Test 2: Two-MATCH pattern with ORDER BY first vertex
-- Sort Key should use raw graphid (a.id) through 2-level chain
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)
    MATCH (b)-[:knows]->(c:Person)
    RETURN a.name
    ORDER BY a
$$) AS (name agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)
    MATCH (b)-[:knows]->(c:Person)
    RETURN a
    ORDER BY a
$$) AS (plan agtype);

--
-- Test 3: Three-MATCH pattern with ORDER BY first vertex
-- Sort Key should use raw graphid (a.id) through 3-level chain
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)
    MATCH (b)-[:knows]->(c:Person)
    MATCH (c)-[:knows]->(d:Person)
    RETURN a.name
    ORDER BY a
$$) AS (name agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)
    MATCH (b)-[:knows]->(c:Person)
    MATCH (c)-[:knows]->(d:Person)
    RETURN a
    ORDER BY a
$$) AS (plan agtype);

--
-- Test 4: ORDER BY on edge
-- Sort Key should use raw graphid (r.id)
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[r:knows]->(b:Person)
    RETURN r
    ORDER BY r
$$) AS (rel agtype);

-- Verify optimization for edge
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[r:knows]->(b:Person)
    RETURN r
    ORDER BY r
$$) AS (plan agtype);

--
-- Test 5: ORDER BY DESC on vertex
-- Sort Key should preserve DESC direction
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (p:Person)
    RETURN p
    ORDER BY p DESC
$$) AS (person agtype);

-- Verify optimization preserves DESC
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (p:Person)
    RETURN p
    ORDER BY p DESC
$$) AS (plan agtype);

--
-- Test 6: Multiple ORDER BY columns (property and vertex)
-- Property sort key unchanged, vertex sort key optimized
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (p:Person)
    RETURN p, p.name
    ORDER BY p.name, p
$$) AS (person agtype, name agtype);

-- Verify second sort key is optimized
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (p:Person)
    RETURN p, p.name
    ORDER BY p.name, p
$$) AS (plan agtype);

--
-- Test 7: OPTIONAL MATCH with ORDER BY
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)
    OPTIONAL MATCH (a)-[:knows]->(b:Person)
    RETURN a, b
    ORDER BY a
    LIMIT 5
$$) AS (a agtype, b agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)
    OPTIONAL MATCH (a)-[:knows]->(b:Person)
    RETURN a, b
    ORDER BY a
    LIMIT 5
$$) AS (plan agtype);

--
-- Test 8: WITH clause and ORDER BY
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (p:Person)
    WITH p
    WHERE p.name IN ['A', 'B', 'C']
    RETURN p
    ORDER BY p
$$) AS (person agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (p:Person)
    WITH p
    WHERE p.name IN ['A', 'B', 'C']
    RETURN p
    ORDER BY p
$$) AS (plan agtype);

--
-- Test 9: Path length 3 - MATCH (a)-[]->(b)-[]->(c)-[]->(d)
-- ORDER BY on first vertex through 1 MATCH
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)-[:knows]->(c:Person)-[:knows]->(d:Person)
    RETURN a.name, b.name, c.name, d.name
    ORDER BY a
$$) AS (a agtype, b agtype, c agtype, d agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)-[:knows]->(c:Person)-[:knows]->(d:Person)
    RETURN a
    ORDER BY a
$$) AS (plan agtype);

--
-- Test 10: Path length 5 - 5 hops in single MATCH
-- ORDER BY on first vertex
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)-[:knows]->(c:Person)
          -[:knows]->(d:Person)-[:knows]->(e:Person)-[:knows]->(f:Person)
    RETURN a.name, f.name
    ORDER BY a
$$) AS (a agtype, f agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)-[:knows]->(c:Person)
          -[:knows]->(d:Person)-[:knows]->(e:Person)-[:knows]->(f:Person)
    RETURN a
    ORDER BY a
$$) AS (plan agtype);

--
-- Test 11: Path length 7 - 7 hops in single MATCH
-- ORDER BY on first vertex
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)-[:knows]->(c:Person)
          -[:knows]->(d:Person)-[:knows]->(e:Person)-[:knows]->(f:Person)
          -[:knows]->(g:Person)-[:knows]->(h:Person)
    RETURN a.name, h.name
    ORDER BY a
$$) AS (a agtype, h agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)-[:knows]->(c:Person)
          -[:knows]->(d:Person)-[:knows]->(e:Person)-[:knows]->(f:Person)
          -[:knows]->(g:Person)-[:knows]->(h:Person)
    RETURN a
    ORDER BY a
$$) AS (plan agtype);

--
-- Test 12: Path length 3 with multiple MATCHes
-- 3 separate MATCH clauses
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)
    MATCH (b)-[:knows]->(c:Person)
    MATCH (c)-[:knows]->(d:Person)
    RETURN a.name, d.name
    ORDER BY a
$$) AS (a agtype, d agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)
    MATCH (b)-[:knows]->(c:Person)
    MATCH (c)-[:knows]->(d:Person)
    RETURN a
    ORDER BY a
$$) AS (plan agtype);

--
-- Test 13: Path length 5 with multiple MATCHes
-- 5 separate MATCH clauses
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)
    MATCH (b)-[:knows]->(c:Person)
    MATCH (c)-[:knows]->(d:Person)
    MATCH (d)-[:knows]->(e:Person)
    MATCH (e)-[:knows]->(f:Person)
    RETURN a.name, f.name
    ORDER BY a
$$) AS (a agtype, f agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)
    MATCH (b)-[:knows]->(c:Person)
    MATCH (c)-[:knows]->(d:Person)
    MATCH (d)-[:knows]->(e:Person)
    MATCH (e)-[:knows]->(f:Person)
    RETURN a
    ORDER BY a
$$) AS (plan agtype);

--
-- Test 14: Path length 7 with multiple MATCHes
-- 7 separate MATCH clauses
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)
    MATCH (b)-[:knows]->(c:Person)
    MATCH (c)-[:knows]->(d:Person)
    MATCH (d)-[:knows]->(e:Person)
    MATCH (e)-[:knows]->(f:Person)
    MATCH (f)-[:knows]->(g:Person)
    MATCH (g)-[:knows]->(h:Person)
    RETURN a.name, h.name
    ORDER BY a
$$) AS (a agtype, h agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)
    MATCH (b)-[:knows]->(c:Person)
    MATCH (c)-[:knows]->(d:Person)
    MATCH (d)-[:knows]->(e:Person)
    MATCH (e)-[:knows]->(f:Person)
    MATCH (f)-[:knows]->(g:Person)
    MATCH (g)-[:knows]->(h:Person)
    RETURN a
    ORDER BY a
$$) AS (plan agtype);

--
-- Test 15: WITH clause passing multiple variables with ORDER BY on multiple vertices
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)
    WITH a, b
    ORDER BY a, b
    RETURN a.name, b.name
$$) AS (a_name agtype, b_name agtype);

-- Verify optimization for multiple sort keys
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)
    WITH a, b
    ORDER BY a, b
    RETURN a.name, b.name
$$) AS (a_name agtype, b_name agtype);

--
-- Test 16: WITH clause with ORDER BY on vertex and edge
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[r:knows]->(b:Person)
    WITH a, r, b
    ORDER BY a, r
    RETURN a.name, b.name
$$) AS (a_name agtype, b_name agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[r:knows]->(b:Person)
    WITH a, r, b
    ORDER BY a, r
    RETURN a.name, b.name
$$) AS (a_name agtype, b_name agtype);

--
-- Test 17: Chained WITH clauses each with ORDER BY
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)
    WITH a, b
    ORDER BY a
    WITH a, b
    ORDER BY b
    RETURN a.name, b.name
$$) AS (a_name agtype, b_name agtype);

-- Verify optimization on chained ORDER BY
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)
    WITH a, b
    ORDER BY a
    WITH a, b
    ORDER BY b
    RETURN a.name, b.name
$$) AS (a_name agtype, b_name agtype);

--
-- Test 18: WITH clause with multiple ORDER BY columns (mixed directions)
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)
    WITH a, b
    ORDER BY a ASC, b DESC
    RETURN a.name, b.name
$$) AS (a_name agtype, b_name agtype);

-- Verify optimization preserves sort directions
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)
    WITH a, b
    ORDER BY a ASC, b DESC
    RETURN a.name, b.name
$$) AS (a_name agtype, b_name agtype);

--
-- Test 19: Three-level chain with ORDER BY at each WITH
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)-[:knows]->(c:Person)
    WITH a, b, c
    ORDER BY a
    WITH a, b, c
    ORDER BY b
    WITH a, b, c
    ORDER BY c
    RETURN a.name, b.name, c.name
$$) AS (a_name agtype, b_name agtype, c_name agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)-[:knows]->(c:Person)
    WITH a, b, c
    ORDER BY a
    WITH a, b, c
    ORDER BY b
    WITH a, b, c
    ORDER BY c
    RETURN a.name, b.name, c.name
$$) AS (a_name agtype, b_name agtype, c_name agtype);

--
-- Test 20: WITH aggregation followed by ORDER BY on vertex
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)
    WITH a, count(b) AS cnt
    ORDER BY a
    RETURN a.name, cnt
$$) AS (a_name agtype, cnt agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)
    WITH a, count(b) AS cnt
    ORDER BY a
    RETURN a.name, cnt
$$) AS (a_name agtype, cnt agtype);

--
-- Test 21: Multiple MATCH with WITH and ORDER BY on multiple vertices
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)
    MATCH (b)-[:knows]->(c:Person)
    WITH a, b, c
    ORDER BY a, b, c
    RETURN a.name, b.name, c.name
$$) AS (a_name agtype, b_name agtype, c_name agtype);

-- Verify optimization for three sort keys
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)
    MATCH (b)-[:knows]->(c:Person)
    WITH a, b, c
    ORDER BY a, b, c
    RETURN a.name, b.name, c.name
$$) AS (a_name agtype, b_name agtype, c_name agtype);

--
-- Test 22: WITH LIMIT and ORDER BY
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[:knows]->(b:Person)
    WITH a, b
    ORDER BY a, b
    LIMIT 3
    RETURN a.name, b.name
$$) AS (a_name agtype, b_name agtype);

-- Verify optimization with LIMIT
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[:knows]->(b:Person)
    WITH a, b
    ORDER BY a, b
    LIMIT 3
    RETURN a.name, b.name
$$) AS (a_name agtype, b_name agtype);

--
-- Test 23: Chained WITH with different ORDER BY columns at each level
--
SELECT * FROM cypher('order_by_opt', $$
    MATCH (a:Person)-[r:knows]->(b:Person)
    WITH a, r, b
    ORDER BY r
    WITH a, r, b
    ORDER BY a, b
    RETURN a.name, b.name
$$) AS (a_name agtype, b_name agtype);

-- Verify optimization
SELECT * FROM cypher('order_by_opt', $$
    EXPLAIN (VERBOSE, COSTS OFF)
    MATCH (a:Person)-[r:knows]->(b:Person)
    WITH a, r, b
    ORDER BY r
    WITH a, r, b
    ORDER BY a, b
    RETURN a.name, b.name
$$) AS (a_name agtype, b_name agtype);

--
-- Cleanup
--
SELECT drop_graph('order_by_opt', true);
