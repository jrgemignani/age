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

LOAD 'age';
SET search_path TO ag_catalog;

--
-- Tests for scalar comparison optimizations (fast path)
-- These tests specifically target the optimized compare_agtype_scalar_containers
-- function which handles scalar-to-scalar comparisons without iterator overhead.
--

-- Integer scalar comparisons (tests fast path with AGTV_INTEGER type)
SELECT agtype_btree_cmp('1'::agtype, '1'::agtype);
SELECT agtype_btree_cmp('1'::agtype, '2'::agtype);
SELECT agtype_btree_cmp('2'::agtype, '1'::agtype);
SELECT agtype_btree_cmp('-1'::agtype, '1'::agtype);
SELECT agtype_btree_cmp('9223372036854775807'::agtype, '-9223372036854775808'::agtype);

-- Float scalar comparisons (tests fast path with AGTV_FLOAT type)
SELECT agtype_btree_cmp('1.5'::agtype, '1.5'::agtype);
SELECT agtype_btree_cmp('1.5'::agtype, '2.5'::agtype);
SELECT agtype_btree_cmp('3.14159'::agtype, '2.71828'::agtype);
SELECT agtype_btree_cmp('-0.0'::agtype, '0.0'::agtype);

-- Mixed integer/float comparisons (tests numeric compatibility in fast path)
SELECT agtype_btree_cmp('1'::agtype, '1.0'::agtype);
SELECT agtype_btree_cmp('2'::agtype, '1.5'::agtype);
SELECT agtype_btree_cmp('1.0'::agtype, '2'::agtype);

-- String scalar comparisons (tests fast path with no-copy string handling)
SELECT agtype_btree_cmp('"abc"'::agtype, '"abc"'::agtype);
SELECT agtype_btree_cmp('"abc"'::agtype, '"abd"'::agtype);
SELECT agtype_btree_cmp('"xyz"'::agtype, '"abc"'::agtype);
SELECT agtype_btree_cmp('""'::agtype, '"a"'::agtype);
SELECT agtype_btree_cmp('"a"'::agtype, '""'::agtype);

-- Long string comparisons (tests no-copy pointer handling)
SELECT agtype_btree_cmp(
    '"abcdefghijklmnopqrstuvwxyz"'::agtype,
    '"abcdefghijklmnopqrstuvwxyz"'::agtype);
SELECT agtype_btree_cmp(
    '"abcdefghijklmnopqrstuvwxyz"'::agtype,
    '"abcdefghijklmnopqrstuvwxya"'::agtype);

-- Null comparisons (tests fast path with AGTV_NULL type)
SELECT agtype_btree_cmp('null'::agtype, 'null'::agtype);
SELECT agtype_btree_cmp('null'::agtype, '1'::agtype);
SELECT agtype_btree_cmp('1'::agtype, 'null'::agtype);

-- Boolean comparisons (tests fast path with AGTV_BOOL type)
SELECT agtype_btree_cmp('true'::agtype, 'true'::agtype);
SELECT agtype_btree_cmp('false'::agtype, 'false'::agtype);
SELECT agtype_btree_cmp('true'::agtype, 'false'::agtype);
SELECT agtype_btree_cmp('false'::agtype, 'true'::agtype);

-- Numeric comparisons (tests fast path with no-copy numeric handling)
SELECT agtype_btree_cmp('1.23456789012345678901234567890::numeric'::agtype,
                        '1.23456789012345678901234567890::numeric'::agtype);
SELECT agtype_btree_cmp('1.23456789012345678901234567890::numeric'::agtype,
                        '1.23456789012345678901234567891::numeric'::agtype);

-- Mixed numeric type comparisons (tests type compatibility logic)
SELECT agtype_btree_cmp('1::numeric'::agtype, '1'::agtype);
SELECT agtype_btree_cmp('1'::agtype, '1.0::numeric'::agtype);
SELECT agtype_btree_cmp('1.5'::agtype, '1.5::numeric'::agtype);

-- Cross-type comparisons (tests type priority ordering)
SELECT agtype_btree_cmp('1'::agtype, '"1"'::agtype);
SELECT agtype_btree_cmp('"1"'::agtype, '1'::agtype);
SELECT agtype_btree_cmp('true'::agtype, '1'::agtype);
SELECT agtype_btree_cmp('1'::agtype, 'true'::agtype);
SELECT agtype_btree_cmp('null'::agtype, '"null"'::agtype);

--
-- Tests for direct agtype comparison functions (agtype_eq, agtype_lt, etc.)
-- These test the optimized fast path for scalar comparisons
--

-- Test agtype_eq with direct agtype arguments (fast path)
SELECT agtype_eq('1'::agtype, '1'::agtype);
SELECT agtype_eq('1'::agtype, '2'::agtype);
SELECT agtype_eq('"hello"'::agtype, '"hello"'::agtype);
SELECT agtype_eq('"hello"'::agtype, '"world"'::agtype);

-- Test agtype_ne with direct agtype arguments (fast path)
SELECT agtype_ne('1'::agtype, '1'::agtype);
SELECT agtype_ne('1'::agtype, '2'::agtype);

-- Test agtype_lt with direct agtype arguments (fast path)
SELECT agtype_lt('1'::agtype, '2'::agtype);
SELECT agtype_lt('2'::agtype, '1'::agtype);
SELECT agtype_lt('1'::agtype, '1'::agtype);

-- Test agtype_gt with direct agtype arguments (fast path)
SELECT agtype_gt('2'::agtype, '1'::agtype);
SELECT agtype_gt('1'::agtype, '2'::agtype);
SELECT agtype_gt('1'::agtype, '1'::agtype);

-- Test agtype_le with direct agtype arguments (fast path)
SELECT agtype_le('1'::agtype, '2'::agtype);
SELECT agtype_le('1'::agtype, '1'::agtype);
SELECT agtype_le('2'::agtype, '1'::agtype);

-- Test agtype_ge with direct agtype arguments (fast path)
SELECT agtype_ge('2'::agtype, '1'::agtype);
SELECT agtype_ge('1'::agtype, '1'::agtype);
SELECT agtype_ge('1'::agtype, '2'::agtype);

-- Test agtype_any_* functions with type conversion (non-fast path)
-- These use variadic args and fall back to extract_variadic_args
SELECT agtype_any_eq(1::bigint, '1'::agtype);
SELECT agtype_any_eq('1'::agtype, 1::bigint);
SELECT agtype_any_lt(1::bigint, '2'::agtype);
SELECT agtype_any_gt('2'::agtype, 1::bigint);

-- Test with SQL NULL (special handling in comparison)
SELECT agtype_eq(NULL::agtype, '1'::agtype);
SELECT agtype_eq('1'::agtype, NULL::agtype);

-- Test agtype null handling (fast path null comparison)
SELECT agtype_eq('null'::agtype, '1'::agtype);
SELECT agtype_eq('1'::agtype, 'null'::agtype);
SELECT agtype_eq('null'::agtype, 'null'::agtype);

--
-- Tests for complex scalar types (VERTEX, EDGE, PATH)
-- These require full deserialization even in the fast path
--

-- Vertex comparison tests
SELECT agtype_btree_cmp(
    '{"id":1, "label":"person", "properties":{"name":"John"}}::vertex'::agtype,
    '{"id":1, "label":"person", "properties":{"name":"John"}}::vertex'::agtype);
SELECT agtype_btree_cmp(
    '{"id":1, "label":"person", "properties":{"name":"John"}}::vertex'::agtype,
    '{"id":2, "label":"person", "properties":{"name":"Jane"}}::vertex'::agtype);

-- Edge comparison tests
SELECT agtype_btree_cmp(
    '{"id":1, "start_id":1, "end_id":2, "label":"knows", "properties":{}}::edge'::agtype,
    '{"id":1, "start_id":1, "end_id":2, "label":"knows", "properties":{}}::edge'::agtype);
SELECT agtype_btree_cmp(
    '{"id":1, "start_id":1, "end_id":2, "label":"knows", "properties":{}}::edge'::agtype,
    '{"id":2, "start_id":1, "end_id":2, "label":"knows", "properties":{}}::edge'::agtype);

--
-- Non-scalar comparison tests (verify iterator path still works correctly)
--

-- Array comparisons (should use iterator path)
SELECT agtype_btree_cmp('[1,2,3]'::agtype, '[1,2,3]'::agtype);
SELECT agtype_btree_cmp('[1,2,3]'::agtype, '[1,2,4]'::agtype);
SELECT agtype_btree_cmp('[1,2]'::agtype, '[1,2,3]'::agtype);

-- Object comparisons (should use iterator path)
SELECT agtype_btree_cmp('{"a":1}'::agtype, '{"a":1}'::agtype);
SELECT agtype_btree_cmp('{"a":1}'::agtype, '{"a":2}'::agtype);
SELECT agtype_btree_cmp('{"a":1}'::agtype, '{"b":1}'::agtype);

-- Mixed scalar/non-scalar comparisons (tests type priority)
SELECT agtype_btree_cmp('1'::agtype, '[1]'::agtype);
SELECT agtype_btree_cmp('[1]'::agtype, '1'::agtype);
SELECT agtype_btree_cmp('"hello"'::agtype, '{"hello":1}'::agtype);
SELECT agtype_btree_cmp('{"hello":1}'::agtype, '"hello"'::agtype);

--
-- Test with Cypher queries to ensure optimization works in context
--

SELECT * FROM create_graph('scalar_optimize_test');

-- Create some test data (count instead of returning vertices to avoid ID differences)
SELECT * FROM cypher('scalar_optimize_test', $$
    CREATE (a:Person {name: 'Alice', age: 30})
    CREATE (b:Person {name: 'Bob', age: 25})
    CREATE (c:Person {name: 'Charlie', age: 35})
    CREATE (d:Person {name: 'David', age: 30})
    RETURN count(*)
$$) AS (cnt agtype);

-- Test ORDER BY with scalar comparisons (exercises the optimized path)
SELECT * FROM cypher('scalar_optimize_test', $$
    MATCH (p:Person)
    RETURN p.name, p.age
    ORDER BY p.age ASC
$$) AS (name agtype, age agtype);

SELECT * FROM cypher('scalar_optimize_test', $$
    MATCH (p:Person)
    RETURN p.name, p.age
    ORDER BY p.age DESC
$$) AS (name agtype, age agtype);

SELECT * FROM cypher('scalar_optimize_test', $$
    MATCH (p:Person)
    RETURN p.name, p.age
    ORDER BY p.name ASC
$$) AS (name agtype, age agtype);

-- Test comparison operators in WHERE clause
SELECT * FROM cypher('scalar_optimize_test', $$
    MATCH (p:Person)
    WHERE p.age > 25
    RETURN p.name, p.age
    ORDER BY p.age
$$) AS (name agtype, age agtype);

SELECT * FROM cypher('scalar_optimize_test', $$
    MATCH (p:Person)
    WHERE p.age = 30
    RETURN p.name, p.age
    ORDER BY p.name
$$) AS (name agtype, age agtype);

SELECT * FROM cypher('scalar_optimize_test', $$
    MATCH (p:Person)
    WHERE p.name < 'Charlie'
    RETURN p.name, p.age
    ORDER BY p.name
$$) AS (name agtype, age agtype);

-- Clean up
SELECT * FROM drop_graph('scalar_optimize_test', true);
