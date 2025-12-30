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

static void ag_deserialize_composite(char *base, enum agtype_value_type type,
                                     agtype_value *result);
static void convert_vertex_to_object(StringInfo buffer, agtentry *pheader,
                                     agtype_value *val);
static void convert_edge_to_object(StringInfo buffer, agtentry *pheader,
                                   agtype_value *val);

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
        convert_vertex_to_object(buffer, &object_ae, scalar_val);

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
        convert_edge_to_object(buffer, &object_ae, scalar_val);

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
 * For AGTV_VERTEX and AGTV_EDGE, populates the new struct format.
 * For AGTV_PATH, uses the object/array format.
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
    /*
     * Use skip_nested = false to recurse into nested containers like
     * path elements, ensuring vertices and edges are properly deserialized.
     */
    while ((tok = agtype_iterator_next(&it, r, false)) != WAGT_DONE)
    {
        parsed_agtype_value = push_agtype_value(
            &parse_state, tok, tok < WAGT_BEGIN_ARRAY ? r : NULL);
    }

    result->type = type;

    /* For vertex and edge, convert from object format to new struct format */
    if (type == AGTV_VERTEX)
    {
        agtype_value *id_val = NULL;
        agtype_value *label_val = NULL;
        agtype_value *props_val = NULL;
        int i;

        /* Verify parsed object */
        if (parsed_agtype_value == NULL ||
            parsed_agtype_value->type != AGTV_OBJECT)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("invalid vertex format: expected object")));
        }

        /* Extract id, label, properties from the parsed object */
        for (i = 0; i < parsed_agtype_value->val.object.num_pairs; i++)
        {
            agtype_pair *pair = &parsed_agtype_value->val.object.pairs[i];
            char *key = pair->key.val.string.val;
            int key_len = pair->key.val.string.len;

            if (key_len == 2 && strncmp(key, "id", 2) == 0)
                id_val = &pair->value;
            else if (key_len == 5 && strncmp(key, "label", 5) == 0)
                label_val = &pair->value;
            else if (key_len == 10 && strncmp(key, "properties", 10) == 0)
                props_val = &pair->value;
        }

        /* Verify all required fields are present */
        if (id_val == NULL || label_val == NULL || props_val == NULL)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("invalid vertex format: missing required field")));
        }

        /* Populate the vertex struct */
        result->val.vertex.id = id_val->val.int_value;
        result->val.vertex.label_id = InvalidOid;
        result->val.vertex.label_len = label_val->val.string.len;
        /* Deep copy the label string */
        result->val.vertex.label = palloc(label_val->val.string.len + 1);
        memcpy(result->val.vertex.label, label_val->val.string.val,
               label_val->val.string.len);
        result->val.vertex.label[label_val->val.string.len] = '\0';

        /* Store properties - allocate a new agtype_value for them */
        result->val.vertex.properties = palloc(sizeof(agtype_value));
        memcpy(result->val.vertex.properties, props_val, sizeof(agtype_value));
    }
    else if (type == AGTV_EDGE)
    {
        agtype_value *id_val = NULL;
        agtype_value *label_val = NULL;
        agtype_value *start_id_val = NULL;
        agtype_value *end_id_val = NULL;
        agtype_value *props_val = NULL;
        int i;

        /* Verify parsed object */
        if (parsed_agtype_value == NULL ||
            parsed_agtype_value->type != AGTV_OBJECT)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("invalid edge format: expected object")));
        }

        /* Extract id, label, start_id, end_id, properties from the parsed object */
        for (i = 0; i < parsed_agtype_value->val.object.num_pairs; i++)
        {
            agtype_pair *pair = &parsed_agtype_value->val.object.pairs[i];
            char *key = pair->key.val.string.val;
            int key_len = pair->key.val.string.len;

            if (key_len == 2 && strncmp(key, "id", 2) == 0)
                id_val = &pair->value;
            else if (key_len == 5 && strncmp(key, "label", 5) == 0)
                label_val = &pair->value;
            else if (key_len == 8 && strncmp(key, "start_id", 8) == 0)
                start_id_val = &pair->value;
            else if (key_len == 6 && strncmp(key, "end_id", 6) == 0)
                end_id_val = &pair->value;
            else if (key_len == 10 && strncmp(key, "properties", 10) == 0)
                props_val = &pair->value;
        }

        /* Verify all required fields are present */
        if (id_val == NULL || label_val == NULL || start_id_val == NULL ||
            end_id_val == NULL || props_val == NULL)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("invalid edge format: missing required field")));
        }

        /* Populate the edge struct */
        result->val.edge.id = id_val->val.int_value;
        result->val.edge.label_id = InvalidOid;
        result->val.edge.start_id = start_id_val->val.int_value;
        result->val.edge.end_id = end_id_val->val.int_value;
        result->val.edge.label_len = label_val->val.string.len;
        /* Deep copy the label string */
        result->val.edge.label = palloc(label_val->val.string.len + 1);
        memcpy(result->val.edge.label, label_val->val.string.val,
               label_val->val.string.len);
        result->val.edge.label[label_val->val.string.len] = '\0';

        /* Store properties - allocate a new agtype_value for them */
        result->val.edge.properties = palloc(sizeof(agtype_value));
        memcpy(result->val.edge.properties, props_val, sizeof(agtype_value));
    }
    else
    {
        /* For PATH and other types, just copy the val union */
        result->val = parsed_agtype_value->val;
    }
}

/*
 * Helper to set up a string key in an agtype_pair.
 */
static void set_pair_key(agtype_pair *pair, const char *key, int len)
{
    pair->key.type = AGTV_STRING;
    pair->key.val.string.len = len;
    pair->key.val.string.val = (char *)key;
}

/*
 * Helper to set up an integer value in an agtype_pair.
 */
static void set_pair_int_value(agtype_pair *pair, int64 value)
{
    pair->value.type = AGTV_INTEGER;
    pair->value.val.int_value = value;
}

/*
 * Helper to set up a string value in an agtype_pair.
 */
static void set_pair_string_value(agtype_pair *pair, const char *str, int len)
{
    pair->value.type = AGTV_STRING;
    pair->value.val.string.len = len;
    pair->value.val.string.val = (char *)str;
}

/*
 * Convert a vertex (new struct format) to on-disk object format.
 * Builds an agtype_value object and uses convert_extended_object.
 * The on-disk format is: {"id": <graphid>, "label": <string>, "properties": <object>}
 */
static void convert_vertex_to_object(StringInfo buffer, agtentry *pheader,
                                     agtype_value *val)
{
    agtype_value obj;
    agtype_pair pairs[3];

    /* Build the object structure */
    obj.type = AGTV_OBJECT;
    obj.val.object.num_pairs = 3;
    obj.val.object.pairs = pairs;

    /* Set up id pair */
    set_pair_key(&pairs[0], "id", 2);
    set_pair_int_value(&pairs[0], val->val.vertex.id);

    /* Set up label pair */
    set_pair_key(&pairs[1], "label", 5);
    set_pair_string_value(&pairs[1], val->val.vertex.label,
                          val->val.vertex.label_len);

    /* Set up properties pair */
    set_pair_key(&pairs[2], "properties", 10);
    memcpy(&pairs[2].value, val->val.vertex.properties, sizeof(agtype_value));

    /* Use existing serialization infrastructure */
    convert_extended_object(buffer, pheader, &obj);
}

/*
 * Convert an edge (new struct format) to on-disk object format.
 * Builds an agtype_value object and uses convert_extended_object.
 * The on-disk format is: {"id": <graphid>, "label": <string>, "end_id": <graphid>,
 *                         "start_id": <graphid>, "properties": <object>}
 */
static void convert_edge_to_object(StringInfo buffer, agtentry *pheader,
                                   agtype_value *val)
{
    agtype_value obj;
    agtype_pair pairs[5];

    /* Build the object structure */
    obj.type = AGTV_OBJECT;
    obj.val.object.num_pairs = 5;
    obj.val.object.pairs = pairs;

    /* Set up pairs in the order expected by AGE: id, label, end_id, start_id, properties */
    set_pair_key(&pairs[0], "id", 2);
    set_pair_int_value(&pairs[0], val->val.edge.id);

    set_pair_key(&pairs[1], "label", 5);
    set_pair_string_value(&pairs[1], val->val.edge.label,
                          val->val.edge.label_len);

    set_pair_key(&pairs[2], "end_id", 6);
    set_pair_int_value(&pairs[2], val->val.edge.end_id);

    set_pair_key(&pairs[3], "start_id", 8);
    set_pair_int_value(&pairs[3], val->val.edge.start_id);

    set_pair_key(&pairs[4], "properties", 10);
    memcpy(&pairs[4].value, val->val.edge.properties, sizeof(agtype_value));

    /* Use existing serialization infrastructure */
    convert_extended_object(buffer, pheader, &obj);
}
