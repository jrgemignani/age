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

#ifndef AG_CYPHER_OPTIMIZER_H
#define AG_CYPHER_OPTIMIZER_H

#include "postgres.h"
#include "nodes/parsenodes.h"

/*
 * Post-transform optimization pass for Cypher queries.
 *
 * This function is called after cypher() transforms are complete.
 * At this point, the Query tree is standard PostgreSQL and can be
 * mutated for optimizations such as:
 *
 * - Optimizing ORDER BY on vertices/edges to use the id field
 * - Other query optimizations specific to graph patterns
 *
 * The function recursively processes the query and all subqueries.
 */
void optimize_cypher_query(Query *query);

#endif /* AG_CYPHER_OPTIMIZER_H */
