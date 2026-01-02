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

#include "utils/agtype_ext.h"

/* define the type and size of the agt_header */
#define AGT_HEADER_TYPE uint32
#define AGT_HEADER_SIZE sizeof(AGT_HEADER_TYPE)

/*
 * Vertex structure: {id, label, properties}
 * Properties is at value index 2 (3rd value in the object)
 */
#define VERTEX_PROPERTIES_INDEX 2

/*
 * Edge structure: {id, label, end_id, start_id, properties}
 * Properties is at value index 4 (5th value in the object)
 */
#define EDGE_PROPERTIES_INDEX 4

static void ag_deserialize_composite(char *base, enum agtype_value_type type,
                                     agtype_value *result);

static short ag_serialize_header(StringInfo buffer, uint32 type)
{
    short padlen;
    int offset;

    padlen = pad_buffer_to_int(buffer);
    offset = reserve_from_buffer(buffer, AGT_HEADER_SIZE);
    *((AGT_HEADER_TYPE *)(buffer->data + offset)) = type;

    return padlen;
}

/*
 * Function serializes the data into the buffer provided.
 * Returns false if the type is not defined. Otherwise, true.
 */
bool ag_serialize_extended_type(StringInfo buffer, agtentry *agtentry,
                                agtype_value *scalar_val)
{
    short padlen;
    int numlen;
    int offset;

    switch (scalar_val->type)
    {
    case AGTV_INTEGER:
        padlen = ag_serialize_header(buffer, AGT_HEADER_INTEGER);

        /* copy in the int_value data */
        numlen = sizeof(int64);
        offset = reserve_from_buffer(buffer, numlen);
        *((int64 *)(buffer->data + offset)) = scalar_val->val.int_value;

        *agtentry = AGTENTRY_IS_AGTYPE | (padlen + numlen + AGT_HEADER_SIZE);
        break;

    case AGTV_FLOAT:
        padlen = ag_serialize_header(buffer, AGT_HEADER_FLOAT);

        /* copy in the float_value data */
        numlen = sizeof(scalar_val->val.float_value);
        offset = reserve_from_buffer(buffer, numlen);
        *((float8 *)(buffer->data + offset)) = scalar_val->val.float_value;

        *agtentry = AGTENTRY_IS_AGTYPE | (padlen + numlen + AGT_HEADER_SIZE);
        break;

    case AGTV_VERTEX:
    {
        uint32 object_ae = 0;

        padlen = ag_serialize_header(buffer, AGT_HEADER_VERTEX);
        convert_extended_object(buffer, &object_ae, scalar_val);

        /*
         * Make sure that the end of the buffer is padded to the next offset and
         * add this padding to the length of the buffer used. This ensures that
         * everything stays aligned and eliminates errors caused by compounded
         * offsets in the deserialization routines.
         */
        object_ae += pad_buffer_to_int(buffer);

        *agtentry = AGTENTRY_IS_AGTYPE |
                    ((AGTENTRY_OFFLENMASK & (int)object_ae) + AGT_HEADER_SIZE);
        break;
    }

    case AGTV_EDGE:
    {
        uint32 object_ae = 0;

        padlen = ag_serialize_header(buffer, AGT_HEADER_EDGE);
        convert_extended_object(buffer, &object_ae, scalar_val);

        /*
         * Make sure that the end of the buffer is padded to the next offset and
         * add this padding to the length of the buffer used. This ensures that
         * everything stays aligned and eliminates errors caused by compounded
         * offsets in the deserialization routines.
         */
        object_ae += pad_buffer_to_int(buffer);

        *agtentry = AGTENTRY_IS_AGTYPE |
                    ((AGTENTRY_OFFLENMASK & (int)object_ae) + AGT_HEADER_SIZE);
        break;
    }

    case AGTV_PATH:
    {
        uint32 object_ae = 0;

        padlen = ag_serialize_header(buffer, AGT_HEADER_PATH);
        convert_extended_array(buffer, &object_ae, scalar_val);

        /*
         * Make sure that the end of the buffer is padded to the next offset and
         * add this padding to the length of the buffer used. This ensures that
         * everything stays aligned and eliminates errors caused by compounded
         * offsets in the deserialization routines.
         */
        object_ae += pad_buffer_to_int(buffer);

        *agtentry = AGTENTRY_IS_AGTYPE |
                    ((AGTENTRY_OFFLENMASK & (int)object_ae) + AGT_HEADER_SIZE);
        break;
    }

    default:
        return false;
    }
    return true;
}

/*
 * Function deserializes the data from the buffer pointed to by base_addr.
 * NOTE: This function writes to the error log and exits for any UNKNOWN
 * AGT_HEADER type.
 */
void ag_deserialize_extended_type(char *base_addr, uint32 offset,
                                  agtype_value *result)
{
    char *base = base_addr + INTALIGN(offset);
    AGT_HEADER_TYPE agt_header = *((AGT_HEADER_TYPE *)base);

    switch (agt_header)
    {
    case AGT_HEADER_INTEGER:
        result->type = AGTV_INTEGER;
        result->val.int_value = *((int64 *)(base + AGT_HEADER_SIZE));
        break;

    case AGT_HEADER_FLOAT:
        result->type = AGTV_FLOAT;
        result->val.float_value = *((float8 *)(base + AGT_HEADER_SIZE));
        break;

    case AGT_HEADER_VERTEX:
        ag_deserialize_composite(base, AGTV_VERTEX, result);
        break;

    case AGT_HEADER_EDGE:
        ag_deserialize_composite(base, AGTV_EDGE, result);
        break;

    case AGT_HEADER_PATH:
        ag_deserialize_composite(base, AGTV_PATH, result);
        break;

    default:
        elog(ERROR, "Invalid AGT header value.");
    }
}

/*
 * Deserializes a composite type.
 */
static void ag_deserialize_composite(char *base, enum agtype_value_type type,
                                     agtype_value *result)
{
    agtype_iterator *it = NULL;
    agtype_iterator_token tok;
    agtype_parse_state *parse_state = NULL;
    agtype_value *r = NULL;
    agtype_value *parsed_agtype_value = NULL;
    /* offset container by the extended type header */
    char *container_base = base + AGT_HEADER_SIZE;

    r = palloc(sizeof(agtype_value));

    it = agtype_iterator_init((agtype_container *)container_base);
    while ((tok = agtype_iterator_next(&it, r, true)) != WAGT_DONE)
    {
        parsed_agtype_value = push_agtype_value(
            &parse_state, tok, tok < WAGT_BEGIN_ARRAY ? r : NULL);
    }

    result->type = type;
    result->val = parsed_agtype_value->val;
}

/*
 * Extract the properties container from a binary vertex or edge directly,
 * without full deserialization.
 *
 * This is an optimization for property access. Instead of deserializing
 * the entire vertex/edge structure just to access a property, we navigate
 * directly to the properties field using the known structure offsets.
 *
 * Parameters:
 *   - container: The agtype_container of a scalar array containing vertex/edge
 *   - result: Output agtype_value to fill with the properties (as AGTV_BINARY)
 *
 * Returns:
 *   - AGT_HEADER_VERTEX if the container is a vertex
 *   - AGT_HEADER_EDGE if the container is an edge
 *   - 0 if the container is not a vertex or edge
 *
 * The result will be filled with AGTV_BINARY pointing to the properties
 * container, which can then be used for property access.
 */
uint32 ag_get_entity_properties_binary(agtype_container *scalar_container,
                                       agtype_value *result)
{
    char *base_addr;
    uint32 offset;
    uint32 agt_header;
    char *entity_base;
    agtype_container *entity_container;
    uint32 num_pairs;
    uint32 properties_index;
    uint32 actual_index;

    /* Scalar array should have exactly 1 element */
    if (!AGTYPE_CONTAINER_IS_ARRAY(scalar_container) ||
        AGTYPE_CONTAINER_SIZE(scalar_container) != 1)
    {
        return 0;
    }

    /* Get the base address for data (after the single agtentry) */
    base_addr = (char *)&scalar_container->children[1];
    offset = get_agtype_offset(scalar_container, 0);

    /* Check if this is an extended type (AGTE_IS_AGTYPE) */
    if (!AGTE_IS_AGTYPE(scalar_container->children[0]))
    {
        return 0;
    }

    /* Read the AGT header to determine vertex or edge */
    entity_base = base_addr + INTALIGN(offset);
    agt_header = *((uint32 *)entity_base);

    if (agt_header == AGT_HEADER_VERTEX)
    {
        properties_index = VERTEX_PROPERTIES_INDEX;
    }
    else if (agt_header == AGT_HEADER_EDGE)
    {
        properties_index = EDGE_PROPERTIES_INDEX;
    }
    else
    {
        /* Not a vertex or edge */
        return 0;
    }

    /* The entity container starts after the AGT header */
    entity_container = (agtype_container *)(entity_base + AGT_HEADER_SIZE);

    /* Verify it's an object with enough pairs */
    if (!AGTYPE_CONTAINER_IS_OBJECT(entity_container))
    {
        return 0;
    }

    num_pairs = AGTYPE_CONTAINER_SIZE(entity_container);
    if (properties_index >= num_pairs)
    {
        return 0;
    }

    /* For objects, values start after keys. Index = num_pairs + value_index */
    actual_index = num_pairs + properties_index;
    base_addr = (char *)&entity_container->children[num_pairs * 2];
    offset = get_agtype_offset(entity_container, actual_index);

    /* The properties should be a container (object) */
    if (!AGTE_IS_CONTAINER(entity_container->children[actual_index]))
    {
        return 0;
    }

    /* Fill result as AGTV_BINARY pointing to the properties container */
    result->type = AGTV_BINARY;
    result->val.binary.data =
        (agtype_container *)(base_addr + INTALIGN(offset));
    result->val.binary.len = get_agtype_length(entity_container, actual_index) -
                             (INTALIGN(offset) - offset);

    return agt_header;
}
