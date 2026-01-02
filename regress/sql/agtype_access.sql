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

--
-- agtype_access_operator regression tests
--
-- Tests for property access optimization:
-- 1. Zero-copy key extraction
-- 2. Direct properties offset access for vertex/edge
--

--
-- Load extension and set path
--
LOAD 'age';
SET search_path TO ag_catalog;

--
-- Create test graph
--
SELECT create_graph('agtype_access_test');

--
-- Create test data with various property types
--
SELECT * FROM cypher('agtype_access_test', $$
    CREATE (n:Person {
        name: 'Alice',
        age: 30,
        height: 5.8,
        active: true,
        tags: ['engineer', 'developer'],
        address: {city: 'Seattle', zip: '98101'},
        metadata: null
    })
    RETURN n
$$) AS (v agtype);

SELECT * FROM cypher('agtype_access_test', $$
    CREATE (n:Person {
        name: 'Bob',
        age: 25,
        scores: [85, 90, 78],
        nested: {level1: {level2: {level3: 'deep'}}}
    })
    RETURN n
$$) AS (v agtype);

-- Create edges with properties
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
    CREATE (a)-[e:KNOWS {since: 2020, weight: 0.95, notes: 'colleagues'}]->(b)
    RETURN e
$$) AS (e agtype);

--
-- Test 1: Basic property access (string, integer, float, boolean)
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person {name: 'Alice'})
    RETURN n.name, n.age, n.height, n.active
$$) AS (name agtype, age agtype, height agtype, active agtype);

--
-- Test 2: Null property access
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person {name: 'Alice'})
    RETURN n.metadata, n.nonexistent
$$) AS (metadata agtype, nonexistent agtype);

--
-- Test 3: Array property access
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person {name: 'Alice'})
    RETURN n.tags, n.tags[0], n.tags[1]
$$) AS (tags agtype, tag0 agtype, tag1 agtype);

-- Negative array index
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person {name: 'Bob'})
    RETURN n.scores[-1], n.scores[-2]
$$) AS (last agtype, second_last agtype);

--
-- Test 4: Nested object property access
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person {name: 'Alice'})
    RETURN n.address, n.address.city, n.address.zip
$$) AS (address agtype, city agtype, zip agtype);

-- Deep nesting
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person {name: 'Bob'})
    RETURN n.nested.level1.level2.level3
$$) AS (deep_value agtype);

--
-- Test 5: Edge property access
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH ()-[e:KNOWS]->()
    RETURN e.since, e.weight, e.notes
$$) AS (since agtype, weight agtype, notes agtype);

--
-- Test 6: Multiple property access from same vertex
-- (Tests optimization for repeated access patterns)
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person)
    RETURN n.name, n.age, n.name, n.age
$$) AS (name1 agtype, age1 agtype, name2 agtype, age2 agtype);

--
-- Test 7: Property access in WHERE clause
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person)
    WHERE n.age > 25
    RETURN n.name
$$) AS (name agtype);

--
-- Test 8: Property access with ORDER BY
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person)
    RETURN n.name, n.age
    ORDER BY n.age DESC
$$) AS (name agtype, age agtype);

--
-- Test 9: Property access in aggregations
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person)
    RETURN count(n.age), sum(n.age), avg(n.age)
$$) AS (cnt agtype, total agtype, average agtype);

--
-- Test 10: Property access with CASE expressions
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person)
    RETURN n.name,
           CASE WHEN n.age > 25 THEN 'senior' ELSE 'junior' END as category
$$) AS (name agtype, category agtype);

--
-- Test 11: Property access on computed/intermediate values
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person {name: 'Alice'})
    WITH n.address as addr
    RETURN addr.city, addr.zip
$$) AS (city agtype, zip agtype);

--
-- Test 12: Mixed array index and property access
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person {name: 'Alice'})
    RETURN n.tags[0]
$$) AS (first_tag agtype);

--
-- Test 13: Reserved field access (id, label)
-- Note: These access the vertex/edge special fields
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person {name: 'Alice'})
    RETURN id(n), label(n)
$$) AS (id agtype, lbl agtype);

SELECT * FROM cypher('agtype_access_test', $$
    MATCH ()-[e:KNOWS]->()
    RETURN id(e), label(e), start_id(e), end_id(e)
$$) AS (id agtype, lbl agtype, start_id agtype, end_id agtype);

--
-- Test 14: Property access with NULL container (should return NULL)
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person {name: 'Alice'})
    RETURN n.nonexistent.nested.deep
$$) AS (result agtype);

--
-- Test 15: Array index out of bounds (should return NULL)
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person {name: 'Alice'})
    RETURN n.tags[100], n.tags[-100]
$$) AS (pos_oob agtype, neg_oob agtype);

--
-- Test 16: Direct SQL access operator tests
-- Tests the agtype_access_operator function directly
--

-- Simple object access
SELECT agtype_access_operator('{"a": 1, "b": 2}'::agtype, '"a"'::agtype);
SELECT agtype_access_operator('{"a": 1, "b": 2}'::agtype, '"b"'::agtype);
SELECT agtype_access_operator('{"a": 1, "b": 2}'::agtype, '"c"'::agtype);

-- Simple array access
SELECT agtype_access_operator('[1, 2, 3]'::agtype, '0'::agtype);
SELECT agtype_access_operator('[1, 2, 3]'::agtype, '1'::agtype);
SELECT agtype_access_operator('[1, 2, 3]'::agtype, '2'::agtype);
SELECT agtype_access_operator('[1, 2, 3]'::agtype, '-1'::agtype);

-- Nested access
SELECT agtype_access_operator('{"a": {"b": {"c": 42}}}'::agtype, '"a"'::agtype, '"b"'::agtype, '"c"'::agtype);

-- Mixed object and array access
SELECT agtype_access_operator('{"arr": [1, 2, 3]}'::agtype, '"arr"'::agtype, '1'::agtype);
SELECT agtype_access_operator('[{"x": 10}, {"x": 20}]'::agtype, '0'::agtype, '"x"'::agtype);

-- NULL handling
SELECT agtype_access_operator('{"a": null}'::agtype, '"a"'::agtype);

--
-- Test 17: Property access with special characters in keys
--
SELECT * FROM cypher('agtype_access_test', $$
    CREATE (n:Special {`special-key`: 'value1', `key.with.dots`: 'value2'})
    RETURN n.`special-key`, n.`key.with.dots`
$$) AS (v1 agtype, v2 agtype);

--
-- Test 18: Large object property access (many properties)
--
SELECT * FROM cypher('agtype_access_test', $$
    CREATE (n:Large {
        p1: 1, p2: 2, p3: 3, p4: 4, p5: 5,
        p6: 6, p7: 7, p8: 8, p9: 9, p10: 10,
        p11: 11, p12: 12, p13: 13, p14: 14, p15: 15,
        p16: 16, p17: 17, p18: 18, p19: 19, p20: 20
    })
    RETURN n.p1, n.p10, n.p20
$$) AS (p1 agtype, p10 agtype, p20 agtype);

--
-- Test 19: Property access with unicode keys
--
SELECT * FROM cypher('agtype_access_test', $$
    CREATE (n:Unicode {名前: 'test', données: 42, emoji🚀: 'rocket'})
    RETURN n.名前, n.données
$$) AS (name_jp agtype, data_fr agtype);

--
-- Test 20: Property access after collection operations
--
SELECT * FROM cypher('agtype_access_test', $$
    MATCH (n:Person)
    WITH collect(n) as people
    RETURN people[0].name, people[1].name
$$) AS (first agtype, second agtype);

--
-- Test 21: Empty object/array property access
--
SELECT agtype_access_operator('{}'::agtype, '"a"'::agtype);
SELECT agtype_access_operator('[]'::agtype, '0'::agtype);

--
-- Test 22: Numeric string keys in objects
--
SELECT agtype_access_operator('{"0": "zero", "1": "one"}'::agtype, '"0"'::agtype);
SELECT agtype_access_operator('{"0": "zero", "1": "one"}'::agtype, '"1"'::agtype);

--
-- Test 23: Boolean as array index (should error)
--
-- This should fail with an appropriate error
SELECT * FROM cypher('agtype_access_test', $$
    RETURN [1,2,3][true]
$$) AS (result agtype);

--
-- Test 24: Float as array index (should error)
--
-- This should fail
SELECT * FROM cypher('agtype_access_test', $$
    RETURN [1,2,3][1.5]
$$) AS (result agtype);

--
-- Test 25: Integer as object key (should error for map access)
--
SELECT * FROM cypher('agtype_access_test', $$
    RETURN {a: 1}[0]
$$) AS (result agtype);

--
-- Test 26: Stress test - deep nesting
--
SELECT agtype_access_operator(
    '{"l1": {"l2": {"l3": {"l4": {"l5": "deep"}}}}}'::agtype,
    '"l1"'::agtype, '"l2"'::agtype, '"l3"'::agtype, '"l4"'::agtype, '"l5"'::agtype
);

--
-- Cleanup
--
SELECT drop_graph('agtype_access_test', true);
