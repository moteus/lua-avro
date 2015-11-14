/* -*- coding: utf-8 -*-
 * ----------------------------------------------------------------------
 * Copyright © 2010-2015, RedJack, LLC.
 * All rights reserved.
 *
 * Please see the COPYING file in this distribution for license details.
 * ----------------------------------------------------------------------
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <avro.h>
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>


int
lua_avro_push_schema(lua_State *L, avro_schema_t schema);

int
lua_avro_push_schema_no_link(lua_State *L, avro_schema_t schema);


/*-----------------------------------------------------------------------
 * Lua access — data
 */

/**
 * The string used to identify the AvroValue class's metatable in the
 * Lua registry.
 */

#define MT_AVRO_VALUE "avro:AvroValue"

static int
lua_return_avro_error(lua_State *L)
{
    lua_pushnil(L);
    lua_pushstring(L, avro_strerror());
    return 2;
}

static int
lua_avro_error(lua_State *L)
{
    lua_pushstring(L, avro_strerror());
    return lua_error(L);
}

#define check(call) \
    do { \
        int __rc; \
        __rc = call; \
        if (__rc != 0) { \
            return lua_avro_error(L); \
        } \
    } while (0)


typedef struct _LuaAvroValue
{
    avro_value_t  value;
    bool  should_decref;
} LuaAvroValue;


int
lua_avro_push_value(lua_State *L, avro_value_t *value, bool should_decref)
{
    LuaAvroValue  *l_value;

    l_value = lua_newuserdata(L, sizeof(LuaAvroValue));
    l_value->value = *value;
    l_value->should_decref = should_decref;
    luaL_getmetatable(L, MT_AVRO_VALUE);
    lua_setmetatable(L, -2);
    return 1;
}


avro_value_t *
lua_avro_get_value(lua_State *L, int index)
{
    LuaAvroValue  *l_value = luaL_checkudata(L, index, MT_AVRO_VALUE);
    return &l_value->value;
}


static int
l_value_raw_value(lua_State *L)
{
    luaL_checkudata(L, 1, MT_AVRO_VALUE);
    return 1;
}


/**
 * Returns the type of an AvroValue instance.
 */

static int
l_value_type(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    lua_pushnumber(L, avro_value_get_type(value));
    return 1;
}


/**
 * Returns the name of the value's schema.
 */

static int
l_value_schema_name(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    avro_schema_t  schema = avro_value_get_schema(value);
    lua_pushstring(L, avro_schema_type_name(schema));
    return 1;
}


/**
 * Returns the size of the value.
 */

static int
l_value_size(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);

    switch (avro_value_get_type(value)) {
        case AVRO_ARRAY:
        case AVRO_MAP:
            {
                size_t  size;
                check(avro_value_get_size(value, &size));
                lua_pushnumber(L, size);
                return 1;
            }

        default:
            lua_pushliteral(L, "Can only get size of array or map");
            return lua_error(L);
    }
}


/**
 * Returns the name of the current union branch.
 */

static int
l_value_discriminant_index(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);

    if (avro_value_get_type(value) != AVRO_UNION) {
        lua_pushliteral(L, "Can't get discriminant of a non-union value");
        return lua_error(L);
    }

    int  discriminant;
    check(avro_value_get_discriminant(value, &discriminant));
    lua_pushinteger(L, discriminant+1);
    return 1;
}


/**
 * Returns the name of the current union branch.
 */

static int
l_value_discriminant(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);

    if (avro_value_get_type(value) != AVRO_UNION) {
        lua_pushliteral(L, "Can't get discriminant of a non-union value");
        return lua_error(L);
    }

    int  discriminant;
    check(avro_value_get_discriminant(value, &discriminant));

    avro_schema_t  union_schema = avro_value_get_schema(value);
    avro_schema_t  branch =
        avro_schema_union_branch(union_schema, discriminant);
    lua_pushstring(L, avro_schema_type_name(branch));
    return 1;
}


/**
 * Returns a JSON-encoded string representing the value.
 */

static int
l_value_tostring(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    char  *json_str = NULL;

    if (avro_value_to_json(value, 1, &json_str))
    {
        lua_pushliteral(L, "Error retrieving JSON encoding for value");
        return lua_error(L);
    }

    lua_pushstring(L, json_str);
    free(json_str);
    return 1;
}


/**
 * Compares two values.
 */

static int
l_value_cmp(lua_State *L)
{
    avro_value_t  *value1 = lua_avro_get_value(L, 1);
    avro_value_t  *value2 = lua_avro_get_value(L, 2);
    int  cmp = avro_value_cmp(value1, value2);
    lua_pushinteger(L, cmp);
    return 1;
}

static int
l_value_lt(lua_State *L)
{
    avro_value_t  *value1 = lua_avro_get_value(L, 1);
    avro_value_t  *value2 = lua_avro_get_value(L, 2);
    int  cmp = avro_value_cmp(value1, value2);
    lua_pushboolean(L, (cmp < 0));
    return 1;
}

static int
l_value_le(lua_State *L)
{
    avro_value_t  *value1 = lua_avro_get_value(L, 1);
    avro_value_t  *value2 = lua_avro_get_value(L, 2);
    int  cmp = avro_value_cmp(value1, value2);
    lua_pushboolean(L, (cmp <= 0));
    return 1;
}


/**
 * Compares two values for equality.
 */

static int
l_value_eq(lua_State *L)
{
    avro_value_t  *value1 = lua_avro_get_value(L, 1);
    avro_value_t  *value2 = lua_avro_get_value(L, 2);
    lua_pushboolean(L, avro_value_equal(value1, value2));
    return 1;
}


/**
 * Copies the contents of one value into another.
 */

static int
l_value_copy_from(lua_State *L)
{
    avro_value_t  *dest = lua_avro_get_value(L, 1);
    avro_value_t  *src = lua_avro_get_value(L, 2);
    lua_pushboolean(L, avro_value_copy(dest, src));
    return 1;
}


/**
 * Sets the source of a resolved reader value.
 */

static int
l_value_set_source(lua_State *L)
{
    LuaAvroValue  *l_value1 = luaL_checkudata(L, 1, MT_AVRO_VALUE);
    avro_value_t  *value2 = lua_avro_get_value(L, 2);

    avro_resolved_reader_set_source(&l_value1->value, value2);
    return 0;
}


/**
 * Sets the destination of a resolved writer value.
 */

static int
l_value_set_dest(lua_State *L)
{
    LuaAvroValue  *l_value1 = luaL_checkudata(L, 1, MT_AVRO_VALUE);
    avro_value_t  *value2 = lua_avro_get_value(L, 2);

    avro_resolved_writer_set_dest(&l_value1->value, value2);
    return 0;
}


/**
 * Resets a value.
 */

static int
l_value_reset(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    check(avro_value_reset(value));
    return 0;
}


/**
 * Select the union branch with the given name, and push a Value wrapper
 * for the branch onto the Lua stack.
 */

static int
select_union_branch(lua_State *L, avro_value_t *value, int branch_index)
{
    int  discriminant;

    if (lua_isnumber(L, branch_index)) {
        discriminant = lua_tointeger(L, branch_index) - 1;
    }

    else if (lua_isstring(L, branch_index)) {
        const char  *branch_name = lua_tostring(L, branch_index);
        avro_schema_t  union_schema = avro_value_get_schema(value);
        avro_schema_t  branch_schema =
            avro_schema_union_branch_by_name
            (union_schema, &discriminant, branch_name);

        if (branch_schema == NULL) {
            lua_pushfstring(L, "No %s branch in union", branch_name);
            return lua_error(L);
        }
    }

    else {
        lua_pushliteral(L, "Can only set string or integer index in union");
        return lua_error(L);
    }

    avro_value_t  branch;
    check(avro_value_set_branch(value, discriminant, &branch));
    lua_avro_push_value(L, &branch, false);
    return 1;
}

/**
 * Extract the contents of an Avro value.  For scalars, we push the
 * equivalent Lua value onto the stack.  For arrays and maps, we
 * retrieve the element the given index.  For records, we retrieve the
 * field with the given name or index.  For unions, we return the
 * current branch.
 */

static int
l_value_get(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);

    switch (avro_value_get_type(value))
    {
      case AVRO_STRING:
        {
            const char  *val = NULL;
            size_t  size = 0;
            check(avro_value_get_string(value, &val, &size));
            /* size contains the NUL terminator */
            lua_pushlstring(L, val, size-1);
            return 1;
        }

      case AVRO_BYTES:
        {
            const void  *val = NULL;
            size_t  size = 0;
            check(avro_value_get_bytes(value, &val, &size));
            lua_pushlstring(L, val, size);
            return 1;
        }

      case AVRO_INT32:
        {
            int32_t  val = 0;
            check(avro_value_get_int(value, &val));
            lua_pushnumber(L, val);
            return 1;
        }

      case AVRO_INT64:
        {
            int64_t  val = 0;
            check(avro_value_get_long(value, &val));
            lua_pushnumber(L, val);
            return 1;
        }

      case AVRO_FLOAT:
        {
            float  val = 0;
            check(avro_value_get_float(value, &val));
            lua_pushnumber(L, val);
            return 1;
        }

      case AVRO_DOUBLE:
        {
            double  val = 0;
            check(avro_value_get_double(value, &val));
            lua_pushnumber(L, val);
            return 1;
        }

      case AVRO_BOOLEAN:
        {
            int  val = 0;
            check(avro_value_get_boolean(value, &val));
            lua_pushboolean(L, val);
            return 1;
        }

      case AVRO_NULL:
        {
            check(avro_value_get_null(value));
            lua_pushnil(L);
            return 1;
        }

      case AVRO_ENUM:
        {
            int  val = 0;
            check(avro_value_get_enum(value, &val));
            avro_schema_t  enum_schema = avro_value_get_schema(value);
            const char  *name = avro_schema_enum_get(enum_schema, val);
            lua_pushstring(L, name);
            return 1;
        }

      case AVRO_FIXED:
        {
            const void  *val = NULL;
            size_t  size = 0;
            check(avro_value_get_fixed(value, &val, &size));
            lua_pushlstring(L, val, size);
            return 1;
        }

      case AVRO_ARRAY:
        {
            lua_Integer  index = luaL_checkinteger(L, 2);
            size_t  array_size;
            check(avro_value_get_size(value, &array_size));

            if ((index < 1) || (index > array_size)) {
                lua_pushnil(L);
                lua_pushliteral(L, "Index out of bounds");
                return 2;
            }

            avro_value_t  element_value;
            check(avro_value_get_by_index(value, index-1, &element_value, NULL));
            lua_avro_push_value(L, &element_value, false);
            return 1;
        }

      case AVRO_MAP:
        {
            if (lua_gettop(L) < 2) {
                lua_pushliteral(L, "Missing index on MapValue:get()");
                return lua_error(L);
            }


            if (lua_isnumber(L, 2)) {
                lua_Integer  index = lua_tointeger(L, 2);
                size_t  map_size;
                check(avro_value_get_size(value, &map_size));

                if ((index < 1) || (index > map_size)) {
                    lua_pushnil(L);
                    lua_pushliteral(L, "Index out of bounds");
                    return 2;
                }

                avro_value_t  element_value = { NULL, NULL };
                const char  *key;
                check(avro_value_get_by_index(value, index-1, &element_value, &key));

                if (element_value.self == NULL) {
                    lua_pushnil(L);
                    lua_pushliteral(L, "Map element doesn't exist");
                    return 2;
                } else {
                    lua_avro_push_value(L, &element_value, false);
                    lua_pushstring(L, key);
                    return 2;
                }
            }

            if (lua_isstring(L, 2)) {
                const char  *key = lua_tostring(L, 2);
                avro_value_t  element_value = { NULL, NULL };
                size_t  index;
                check(avro_value_get_by_name(value, key, &element_value, &index));

                if (element_value.self == NULL) {
                    lua_pushnil(L);
                    lua_pushliteral(L, "Map element doesn't exist");
                    return 2;
                } else {
                    lua_avro_push_value(L, &element_value, false);
                    lua_pushinteger(L, index);
                    return 2;
                }
            }

            lua_pushliteral(L, "Can only get string or integer index from map");
            return lua_error(L);
        }

      case AVRO_RECORD:
        {
            if (lua_gettop(L) < 2) {
                lua_pushliteral(L, "Missing index on RecordValue:get()");
                return lua_error(L);
            }

            if (lua_isnumber(L, 2)) {
                lua_Integer  index = lua_tointeger(L, 2);
                avro_value_t  field_value = { NULL, NULL };
                check(avro_value_get_by_index(value, index-1, &field_value, NULL));

                if (field_value.self == NULL) {
                    lua_pushnil(L);
                    lua_pushliteral(L, "Record field doesn't exist");
                    return 2;
                } else {
                    lua_avro_push_value(L, &field_value, false);
                    return 1;
                }
            }

            if (lua_isstring(L, 2)) {
                const char  *key = lua_tostring(L, 2);
                avro_value_t  field_value = { NULL, NULL };
                check(avro_value_get_by_name(value, key, &field_value, NULL));

                if (field_value.self == NULL) {
                    lua_pushnil(L);
                    lua_pushliteral(L, "Record field doesn't exist");
                    return 2;
                } else {
                    lua_avro_push_value(L, &field_value, false);
                    return 1;
                }
            }

            lua_pushliteral(L, "Can only get string or integer index from record");
            return lua_error(L);
        }

      case AVRO_UNION:
        {
            if (lua_gettop(L) < 2 || lua_isnil(L, 2)) {
                avro_value_t  branch;
                check(avro_value_get_current_branch(value, &branch));
                lua_avro_push_value(L, &branch, false);
                return 1;
            } else {
                select_union_branch(L, value, 2);
                return 1;
            }
        }

      default:
        return luaL_error(L, "Don't know how to get from value type %d",
                          avro_value_get_type(value));
    }
}


/**
 * Returns the hash of an AvroValue instance.
 */

static int
l_value_hash(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    uint32_t  hash = avro_value_hash(value);
    lua_pushinteger(L, hash);
    return 1;
}


/**
 * Sets the value value of an Avro scalar.  If the value is not a
 * scalar, we raise a Lua error.
 */

static int
l_value_set(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);

    switch (avro_value_get_type(value))
    {
      case AVRO_STRING:
        {
            size_t  str_len;
            const char  *str = luaL_checklstring(L, 2, &str_len);
            /* value length must include NUL terminator */
            check(avro_value_set_string_len(value, (char *) str, str_len+1));
            return 0;
        }

      case AVRO_BYTES:
        {
            size_t  len;
            const char  *buf = luaL_checklstring(L, 2, &len);
            check(avro_value_set_bytes(value, (void *) buf, len));
            return 0;
        }

      case AVRO_INT32:
        {
            lua_Integer  i = luaL_checkinteger(L, 2);
            check(avro_value_set_int(value, i));
            return 0;
        }

      case AVRO_INT64:
        {
            long  l = luaL_checklong(L, 2);
            check(avro_value_set_long(value, l));
            return 0;
        }

      case AVRO_FLOAT:
        {
            lua_Number  n = luaL_checknumber(L, 2);
            check(avro_value_set_float(value, (float) n));
            return 0;
        }

      case AVRO_DOUBLE:
        {
            lua_Number  n = luaL_checknumber(L, 2);
            check(avro_value_set_double(value, (double) n));
            return 0;
        }

      case AVRO_BOOLEAN:
        {
            int  b = lua_toboolean(L, 2);
            check(avro_value_set_boolean(value, b));
            return 0;
        }

      case AVRO_NULL:
        {
            check(avro_value_set_null(value));
            return 0;
        }

      case AVRO_ENUM:
        {
            int  symbol_value;

            if (lua_isnumber(L, 2)) {
                symbol_value = lua_tointeger(L, 2) - 1;
            }

            else {
                const char  *symbol = luaL_checkstring(L, 2);
                avro_schema_t  enum_schema = avro_value_get_schema(value);
                symbol_value = avro_schema_enum_get_by_name(enum_schema, symbol);
                if (symbol_value < 0) {
                    return luaL_error(L, "No symbol named %s", symbol);
                }
            }

            check(avro_value_set_enum(value, symbol_value));
            return 0;
        }

      case AVRO_FIXED:
        {
            size_t  len = 0;
            const char  *buf = luaL_checklstring(L, 2, &len);
            check(avro_value_set_fixed(value, (void *) buf, len));
            return 0;
        }

      case AVRO_MAP:
        {
            const char  *key = luaL_checkstring(L, 2);
            avro_value_t  element_value;
            check(avro_value_add(value, key, &element_value, NULL, NULL));
            lua_avro_push_value(L, &element_value, false);
            return 1;
        }

      case AVRO_UNION:
        {
            if (lua_gettop(L) < 2) {
                lua_pushliteral(L, "Missing index on UnionValue:set()");
                return lua_error(L);
            }

            select_union_branch(L, value, 2);
            return 1;
        }

      default:
        return luaL_error(L, "Don't know how to set in value type %d",
                          avro_value_get_type(value));
    }
}


/**
 * Fills in the contents of an Avro value from a pure-Lua AST.  For
 * scalars, we expect a compatible Lua scalar value.  For maps and
 * records, we expect a table.  For arrays, we expect an array-like
 * table.  For unions, we expect a scalar nil (if the union contains a
 * null schema), or a single-element table whose key is the name of one
 * of the union schemas.
 */

static int
l_value_set_from_ast(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);

    switch (avro_value_get_type(value))
    {
        case AVRO_BOOLEAN:
        case AVRO_NULL:
        case AVRO_ENUM:
            return l_value_set(L);

        case AVRO_BYTES:
        case AVRO_STRING:
        case AVRO_FIXED:
            {
                /* Force parameter 2 to be a string */
                luaL_checklstring(L, 2, NULL);
                return l_value_set(L);
            }

        case AVRO_DOUBLE:
        case AVRO_FLOAT:
        case AVRO_INT32:
        case AVRO_INT64:
            {
                lua_pushvalue(L, 1);
                lua_pushnumber(L, luaL_checknumber(L, 2));
                return l_value_set(L);
            }

        case AVRO_ARRAY:
            {
                size_t  elements = lua_objlen(L, 2);
                size_t  i;

                check(avro_value_reset(value));

                for (i = 0; i < elements; i++) {
                    avro_value_t  child;
                    check(avro_value_append(value, &child, NULL));
                    lua_pushcfunction(L, l_value_set_from_ast);
                    lua_avro_push_value(L, &child, false);
                    lua_rawgeti(L, 2, i+1);
                    lua_call(L, 2, 0);
                }

                return 0;
            }

        case AVRO_MAP:
            {
                lua_pushnil(L);
                while (lua_next(L, -2) != 0) {
                    /* Stack is now: -3 AST table; -2 key; -1 value */

                    /*
                     * Push a copy of the key so we can safely call
                     * lua_tolstring to get a string key
                     */

                    const char  *key;
                    lua_pushvalue(L, -2);
                    key = lua_tostring(L, -1);

                    /* Stack is now:
                     * -4 AST table
                     * -3 key
                     * -2 value
                     * -1 copied string key
                     */

                    avro_value_t  child;
                    check(avro_value_add(value, key, &child, NULL, NULL));
                    lua_pushcfunction(L, l_value_set_from_ast);
                    lua_avro_push_value(L, &child, false);
                    lua_pushvalue(L, -5);
                    lua_rawget(L, -7);
                    lua_call(L, 2, 0);

                    /* Pop off value and copied key to continue loop */
                    lua_pop(L, 2);
                }

                return 0;
            }

        case AVRO_RECORD:
            {
                lua_pushnil(L);
                while (lua_next(L, -2) != 0) {
                    /* Stack is now: -3 AST table; -2 key; -1 value */

                    /*
                     * Call the get() method to retrieve the field with
                     * the given index.
                     */

                    lua_pushcfunction(L, l_value_get);
                    lua_pushvalue(L, 1);
                    lua_pushvalue(L, -4);
                    lua_call(L, 2, 1);

                    /* Stack is now:
                     * -4 AST table
                     * -3 key
                     * -2 value
                     * -1 field
                     */

                    lua_pushcfunction(L, l_value_set_from_ast);
                    lua_pushvalue(L, -2);  /* field */
                    lua_pushvalue(L, -4);  /* value */
                    lua_call(L, 2, 0);

                    /* Pop off value and field to continue loop */
                    lua_pop(L, 2);
                }

                return 0;
            }

        case AVRO_UNION:
            {
                if (lua_isnil(L, 2)) {
                    lua_pushcfunction(L, l_value_set_from_ast);
                    lua_pushliteral(L, "null");
                    select_union_branch(L, value, -1);
                    lua_replace(L, -2);
                    lua_pushnil(L);
                    lua_call(L, 2, 0);
                    return 0;
                }

                lua_pushnil(L);
                if (lua_next(L, 2) == 0) {
                    lua_pushliteral(L, "Union AST must have exactly one element");
                    return lua_error(L);
                }

                lua_pushcfunction(L, l_value_set_from_ast);
                select_union_branch(L, value, -3);
                lua_pushvalue(L, -3);
                lua_call(L, 2, 0);

                return 0;
            }

        default:
            lua_pushliteral(L, "Unknown Avro value type");
            return lua_error(L);
    }
}


/**
 * Adds a new element to an Avro map.  The first parameter is always the
 * key of the new element.  If called with two parameter, then the map
 * values must be scalars, and the parameter is used as the value of the
 * new element.  If called with one parameter, then the map can contain
 * any kind of value.  In both cases, we return the AvroValue for the
 * new element.
 */

static int
l_value_add(lua_State *L)
{
    int  nargs = lua_gettop(L);
    avro_value_t  *value = lua_avro_get_value(L, 1);
    const char  *key = luaL_checkstring(L, 2);

    if (avro_value_get_type(value) != AVRO_MAP) {
        lua_pushliteral(L, "Can only add to an map");
        return lua_error(L);
    }

    if (nargs > 3) {
        lua_pushliteral(L, "Bad number of arguments to AvroValue:add");
        return lua_error(L);
    }

    avro_value_t  element;
    check(avro_value_add(value, key, &element, NULL, NULL));
    lua_avro_push_value(L, &element, false);

    /*
     * Otherwise just return the new element value.
     */

    return 1;
}


/**
 * Appends a new element to an Avro array.  If called with one
 * parameter, then the array must contain scalars, and the parameter is
 * used as the value of the new element.  If called with no parameters,
 * then the array can contain any kind of element.  In both cases, we
 * return the AvroValue for the new element.
 */

static int
l_value_append(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);

    if (avro_value_get_type(value) != AVRO_ARRAY) {
        lua_pushliteral(L, "Can only append to an array");
        return lua_error(L);
    }

    avro_value_t  element;
    check(avro_value_append(value, &element, NULL));
    lua_avro_push_value(L, &element, false);
    return 1;
}


/**
 * Iterates through the elements of an Avro array or map.  The result of
 * this function can be used as a for loop iterator.  For arrays, the
 * iterator behaves like the builtin ipairs function, returning [i,
 * element] pairs during each iteration.  For maps, it behaves like the
 * builtin pairs function, returning [key, element] pairs.  In both
 * cases, if the elements are scalars, these will be translated into the
 * Lua equivalent; if they're compound value objects, you'll get an
 * AvroValue instance.
 */

typedef struct _Iterator
{
    bool  no_scalar;
    avro_value_t  *value;
    size_t  next_index;
} Iterator;

#define MT_ITERATOR "sawmill:AvroValue:iterator"

static void
create_iterator(lua_State *L, avro_value_t *value, bool no_scalar)
{
    lua_newuserdata(L, sizeof(Iterator));
    Iterator  *state = lua_touserdata(L, -1);
    state->no_scalar = no_scalar;
    state->value = value;
    state->next_index = 0;
    luaL_getmetatable(L, MT_ITERATOR);
    lua_setmetatable(L, -2);
}

static int
iterator_gc(lua_State *L)
{
    Iterator  *state = luaL_checkudata(L, 1, MT_ITERATOR);
    state->value = NULL;
    return 0;
}

static int
iterate_array(lua_State *L)
{
    Iterator  *state = luaL_checkudata(L, 1, MT_ITERATOR);
    size_t  length;
    check(avro_value_get_size(state->value, &length));

    /*
     * next_index is the 0-based avro index, not the 1-based Lua index.
     */

    if (state->next_index >= length) {
        return 0;
    }

    avro_value_t  element;
    check(avro_value_get_by_index(state->value, state->next_index, &element, NULL));
    lua_pushinteger(L, state->next_index+1);
    lua_avro_push_value(L, &element, false);

    state->next_index++;
    return 2;
}

static int
iterate_map(lua_State *L)
{
    Iterator  *state = luaL_checkudata(L, 1, MT_ITERATOR);
    size_t  length;
    check(avro_value_get_size(state->value, &length));

    /*
     * next_index is the 0-based avro index, not the 1-based Lua index.
     */

    if (state->next_index >= length) {
        return 0;
    }

    const char  *key = NULL;
    avro_value_t  element;
    check(avro_value_get_by_index(state->value, state->next_index, &element, &key));

    lua_pushstring(L, key);
    lua_avro_push_value(L, &element, false);

    state->next_index++;
    return 2;
}

static int
l_value_iterate(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    avro_type_t  value_type = avro_value_get_type(value);
    int  no_scalar = lua_toboolean(L, 2);

    if (value_type == AVRO_ARRAY) {
        lua_pushcfunction(L, iterate_array);
        create_iterator(L, value, no_scalar);
        lua_pushnil(L);
        return 3;
    }

    if (value_type == AVRO_MAP) {
        lua_pushcfunction(L, iterate_map);
        create_iterator(L, value, no_scalar);
        lua_pushnil(L);
        return 3;
    }

    lua_pushliteral(L, "Can only iterate through arrays and maps");
    return lua_error(L);
}


/**
 * Encode an Avro value using the binary encoding.  Returns the result
 * as a Lua string.
 */

static int
l_value_encode(lua_State *L)
{
    static char  static_buf[65536];

    avro_value_t  *value = lua_avro_get_value(L, 1);

    size_t  size = 0;
    check(avro_value_sizeof(value, &size));

    int  result;
    char  *buf;
    bool  free_buf;

    if (size <= sizeof(static_buf)) {
        buf = static_buf;
        free_buf = false;
    } else {
        buf = malloc(size);
        if (buf == NULL) {
            lua_pushnil(L);
            lua_pushliteral(L, "Out of memory");
            return 2;
        }
        free_buf = true;
    }

    avro_writer_t  writer = avro_writer_memory(buf, size);
    result = avro_value_write(writer, value);
    avro_writer_free(writer);

    if (result) {
        if (free_buf) {
            free(buf);
        }
        return lua_return_avro_error(L);
    }

    lua_pushlstring(L, buf, size);
    if (free_buf) {
        free(buf);
    }
    return 1;
}


/**
 * Return the length of the binary encoding of the value.
 */

static int
l_value_encoded_size(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);

    size_t  size = 0;
    check(avro_value_sizeof(value, &size));

    lua_pushinteger(L, size);
    return 1;
}


/**
 * Encode an Avro value using the binary encoding.  The result is placed
 * into the given memory region, which is provided as a light user data
 * and a size.  There's no safety checking here; to make it easier to
 * not include this function in sandboxes, it's exposed as a global
 * function in the "avro" package, and not as a method of the AvroValue
 * class.
 */

static int
l_value_encode_raw(lua_State *L)
{
    avro_value_t  *value = lua_avro_get_value(L, 1);
    if (!lua_islightuserdata(L, 2)) {
        return luaL_error(L, "Destination buffer should be a light userdata");
    }
    void  *buf = lua_touserdata(L, 2);
    size_t  size = luaL_checkinteger(L, 3);

    avro_writer_t  writer = avro_writer_memory(buf, size);
    int  result = avro_value_write(writer, value);
    avro_writer_free(writer);

    if (result) {
        lua_pushboolean(L, false);
        lua_pushstring(L, avro_strerror());
        return 2;
    }

    lua_pushboolean(L, true);
    return 1;
}


/**
 * Finalizes an AvroValue instance.
 */

static int
l_value_release(lua_State *L)
{
    LuaAvroValue  *l_value = luaL_checkudata(L, 1, MT_AVRO_VALUE);
    if (l_value->should_decref && l_value->value.self != NULL) {
        avro_value_decref(&l_value->value);
    }
    l_value->value.iface = NULL;
    l_value->value.self = NULL;
    l_value->should_decref = false;
    return 0;
}


/*-----------------------------------------------------------------------
 * Lua access — schemas
 */

/**
 * The string used to identify the AvroSchema class's metatable in the
 * Lua registry.
 */

#define MT_AVRO_SCHEMA "avro:AvroSchema"

typedef struct _LuaAvroSchema
{
    avro_schema_t  schema;
    avro_value_iface_t  *iface;
} LuaAvroSchema;


int
lua_avro_push_schema(lua_State *L, avro_schema_t schema)
{
    LuaAvroSchema  *l_schema;
    l_schema = lua_newuserdata(L, sizeof(LuaAvroSchema));
    l_schema->schema = avro_schema_incref(schema);
    l_schema->iface = NULL;
    luaL_getmetatable(L, MT_AVRO_SCHEMA);
    lua_setmetatable(L, -2);
    return 1;
}

int
lua_avro_push_schema_no_link(lua_State *L, avro_schema_t schema)
{
    while (is_avro_link(schema)) {
        schema = avro_schema_link_target(schema);
    }
    return lua_avro_push_schema(L, schema);
}


avro_schema_t
lua_avro_get_schema(lua_State *L, int index)
{
    lua_pushliteral(L, "raw_schema");
    lua_gettable(L, index);
    lua_pushvalue(L, index);
    lua_call(L, 1, 1);
    LuaAvroSchema  *l_schema = luaL_checkudata(L, -1, MT_AVRO_SCHEMA);
    lua_pop(L, 1);
    return l_schema->schema;
}

avro_schema_t
lua_avro_get_raw_schema(lua_State *L, int index)
{
    LuaAvroSchema  *l_schema = luaL_checkudata(L, index, MT_AVRO_SCHEMA);
    return l_schema->schema;
}


static int
l_new_raw_schema(lua_State *L)
{
    avro_schema_t  schema = lua_touserdata(L, 1);
    if (schema == NULL) {
        lua_pushliteral(L, "Cannot create NULL schema wrapper");
        return lua_error(L);
    }
    lua_avro_push_schema(L, schema);
    lua_pushlightuserdata(L, schema);
    return 2;
}


/**
 * Creates a new AvroValue for the given schema.
 */

static int
l_schema_new_raw_value(lua_State *L)
{
    LuaAvroSchema  *l_schema = luaL_checkudata(L, 1, MT_AVRO_SCHEMA);
    if (l_schema->iface == NULL) {
        l_schema->iface = avro_generic_class_from_schema(l_schema->schema);
        if (l_schema->iface == NULL) {
            lua_pushstring(L, avro_strerror());
            return lua_error(L);
        }
    }

    if (lua_gettop(L) >= 2) {
        LuaAvroValue  *l_value = luaL_checkudata(L, 2, MT_AVRO_VALUE);
        if (l_value->should_decref && l_value->value.self != NULL) {
            avro_value_decref(&l_value->value);
        }
        check(avro_generic_value_new(l_schema->iface, &l_value->value));
        l_value->should_decref = true;
        lua_pushvalue(L, 2);
    } else {
        avro_value_t  value;
        check(avro_generic_value_new(l_schema->iface, &value));
        lua_avro_push_value(L, &value, true);
    }
    return 1;
}


/**
 * Returns the type of an AvroSchema instance.
 */

static int
l_schema_type(lua_State *L)
{
    avro_schema_t  schema = lua_avro_get_raw_schema(L, 1);
    lua_pushnumber(L, avro_typeof(schema));
    return 1;
}


/**
 * Returns the name of an AvroSchema instance.
 */

static int
l_schema_name(lua_State *L)
{
    avro_schema_t  schema = lua_avro_get_raw_schema(L, 1);
    lua_pushstring(L, avro_schema_type_name(schema));
    return 1;
}


/**
 * Finalizes an AvroSchema instance.
 */

static int
l_schema_gc(lua_State *L)
{
    LuaAvroSchema  *l_schema = luaL_checkudata(L, 1, MT_AVRO_SCHEMA);
    if (l_schema->schema != NULL) {
        avro_schema_decref(l_schema->schema);
        l_schema->schema = NULL;
    }
    if (l_schema->iface != NULL) {
        avro_value_iface_decref(l_schema->iface);
        l_schema->iface = NULL;
    }
    return 0;
}


/**
 * Creates a new AvroSchema instance from a JSON schema string.
 */

static int
l_schema_new(lua_State *L)
{
    if (lua_isstring(L, 1)) {
        size_t  json_len;
        const char  *json_str = lua_tolstring(L, 1, &json_len);
        avro_schema_t  schema;

        /* First check for the primitive types */
        if (strcmp(json_str, "boolean") == 0) {
            schema = avro_schema_boolean();
        }

        else if (strcmp(json_str, "bytes") == 0) {
            schema = avro_schema_bytes();
        }

        else if (strcmp(json_str, "double") == 0) {
            schema = avro_schema_double();
        }

        else if (strcmp(json_str, "float") == 0) {
            schema = avro_schema_float();
        }

        else if (strcmp(json_str, "int") == 0) {
            schema = avro_schema_int();
        }

        else if (strcmp(json_str, "long") == 0) {
            schema = avro_schema_long();
        }

        else if (strcmp(json_str, "null") == 0) {
            schema = avro_schema_null();
        }

        else if (strcmp(json_str, "string") == 0) {
            schema = avro_schema_string();
        }

        /* Otherwise assume it's JSON */

        else {
            avro_schema_error_t  schema_error;
            check(avro_schema_from_json(json_str, json_len, &schema, &schema_error));
        }

        lua_avro_push_schema(L, schema);
        avro_schema_decref(schema);
        lua_pushlightuserdata(L, schema);
        return 2;
    }

    if (lua_isuserdata(L, 1)) {
        if (lua_getmetatable(L, 1)) {
            lua_getfield(L, LUA_REGISTRYINDEX, MT_AVRO_SCHEMA);
            if (lua_rawequal(L, -1, -2)) {
                /* This is already a schema object, so just return it. */
                lua_pop(L, 2);  /* remove both metatables */
                LuaAvroSchema  *l_schema = lua_touserdata(L, 1);
                lua_pushlightuserdata(L, l_schema->schema);
                return 2;
            }
        }
    }

    lua_pushliteral(L, "Invalid input to Schema function");
    return lua_error(L);
}


/*-----------------------------------------------------------------------
 * Lua access — resolved readers
 */

/**
 * The string used to identify the AvroResolvedReader class's metatable
 * in the Lua registry.
 */

#define MT_AVRO_RESOLVED_READER "avro:AvroResolvedReader"

typedef struct _LuaAvroResolvedReader
{
    avro_value_iface_t  *resolver;
} LuaAvroResolvedReader;


int
lua_avro_push_resolved_reader(lua_State *L, avro_value_iface_t *resolver)
{
    LuaAvroResolvedReader  *l_resolver;

    l_resolver = lua_newuserdata(L, sizeof(LuaAvroResolvedReader));
    l_resolver->resolver = resolver;
    luaL_getmetatable(L, MT_AVRO_RESOLVED_READER);
    lua_setmetatable(L, -2);
    return 1;
}


avro_value_iface_t *
lua_avro_get_resolved_reader(lua_State *L, int index)
{
    LuaAvroResolvedReader  *l_resolver =
        luaL_checkudata(L, index, MT_AVRO_RESOLVED_READER);
    return l_resolver->resolver;
}


/**
 * Creates a new AvroResolvedReader for the given schemas.
 */

static int
l_resolved_reader_new(lua_State *L)
{
    avro_schema_t  writer_schema = lua_avro_get_schema(L, 1);
    avro_schema_t  reader_schema = lua_avro_get_schema(L, 2);
    avro_value_iface_t  *resolver =
        avro_resolved_reader_new(writer_schema, reader_schema);
    if (resolver == NULL) {
        return lua_return_avro_error(L);
    } else {
        lua_avro_push_resolved_reader(L, resolver);
        return 1;
    }
}


/**
 * Finalizes an AvroResolvedReader instance.
 */

static int
l_resolved_reader_gc(lua_State *L)
{
    LuaAvroResolvedReader  *l_resolver =
        luaL_checkudata(L, 1, MT_AVRO_RESOLVED_READER);
    if (l_resolver->resolver != NULL) {
        avro_value_iface_decref(l_resolver->resolver);
        l_resolver->resolver = NULL;
    }
    return 0;
}


/**
 * Creates a new AvroValue for the given resolved reader.
 */

static int
l_resolved_reader_new_raw_value(lua_State *L)
{
    avro_value_iface_t  *resolver = lua_avro_get_resolved_reader(L, 1);
    avro_value_t  value;
    check(avro_resolved_reader_new_value(resolver, &value));
    lua_avro_push_value(L, &value, true);
    return 1;
}


/*-----------------------------------------------------------------------
 * Lua access — resolved writers
 */

/**
 * The string used to identify the AvroResolvedWriter class's metatable
 * in the Lua registry.
 */

#define MT_AVRO_RESOLVED_WRITER "avro:AvroResolvedWriter"

typedef struct _LuaAvroResolvedWriter
{
    avro_value_iface_t  *resolver;
    avro_value_t  value;
} LuaAvroResolvedWriter;


int
lua_avro_push_resolved_writer(lua_State *L, avro_value_iface_t *resolver)
{
    LuaAvroResolvedWriter  *l_resolver;

    l_resolver = lua_newuserdata(L, sizeof(LuaAvroResolvedWriter));
    l_resolver->resolver = resolver;
    avro_resolved_writer_new_value(resolver, &l_resolver->value);
    luaL_getmetatable(L, MT_AVRO_RESOLVED_WRITER);
    lua_setmetatable(L, -2);
    return 1;
}


avro_value_iface_t *
lua_avro_get_resolved_writer(lua_State *L, int index)
{
    LuaAvroResolvedWriter  *l_resolver =
        luaL_checkudata(L, index, MT_AVRO_RESOLVED_WRITER);
    return l_resolver->resolver;
}


/**
 * Creates a new AvroResolvedWriter for the given schemas.
 */

static int
l_resolved_writer_new(lua_State *L)
{
    avro_schema_t  writer_schema = lua_avro_get_schema(L, 1);
    avro_schema_t  reader_schema = lua_avro_get_schema(L, 2);
    avro_value_iface_t  *resolver =
        avro_resolved_writer_new(writer_schema, reader_schema);
    if (resolver == NULL) {
        return lua_return_avro_error(L);
    } else {
        lua_avro_push_resolved_writer(L, resolver);
        return 1;
    }
}


/**
 * Finalizes an AvroResolvedWriter instance.
 */

static int
l_resolved_writer_gc(lua_State *L)
{
    LuaAvroResolvedWriter  *l_resolver =
        luaL_checkudata(L, 1, MT_AVRO_RESOLVED_WRITER);
    if (l_resolver->value.self != NULL) {
        avro_value_decref(&l_resolver->value);
        l_resolver->value.iface = NULL;
        l_resolver->value.self = NULL;
    }
    if (l_resolver->resolver != NULL) {
        avro_value_iface_decref(l_resolver->resolver);
        l_resolver->resolver = NULL;
    }
    return 0;
}


/**
 * Creates a new AvroValue for the given resolved writer.
 */

static int
l_resolved_writer_new_raw_value(lua_State *L)
{
    avro_value_iface_t  *resolver = lua_avro_get_resolved_writer(L, 1);
    avro_value_t  value;
    check(avro_resolved_writer_new_value(resolver, &value));
    lua_avro_push_value(L, &value, true);
    return 1;
}


/**
 * Decode an Avro value using the given resolver.
 */

static int
l_resolved_writer_decode(lua_State *L)
{
    LuaAvroResolvedWriter  *l_resolver =
        luaL_checkudata(L, 1, MT_AVRO_RESOLVED_WRITER);
    size_t  size = 0;
    const char  *buf = luaL_checklstring(L, 2, &size);
    avro_value_t  *value = lua_avro_get_value(L, 3);

    avro_reader_t  reader = avro_reader_memory(buf, size);
    avro_resolved_writer_set_dest(&l_resolver->value, value);
    int rc = avro_value_read(reader, &l_resolver->value);
    avro_reader_free(reader);

    if (rc != 0) {
        return lua_return_avro_error(L);
    }

    lua_pushboolean(L, true);
    return 1;
}


/**
 * Decode an Avro value, using the binary encoding, from the given
 * memory region, which is provided as a light user data and a size.
 * There's no safety checking here; to make it easier to not include
 * this function in sandboxes, it's exposed as a global function in the
 * "avro" package, and not as a method of the AvroValue class.
 */

static int
l_value_decode_raw(lua_State *L)
{
    LuaAvroResolvedWriter  *l_resolver =
        luaL_checkudata(L, 1, MT_AVRO_RESOLVED_WRITER);
    if (!lua_islightuserdata(L, 2)) {
        return luaL_error(L, "Destination buffer should be a light userdata");
    }
    void  *buf = lua_touserdata(L, 2);
    size_t  size = luaL_checkinteger(L, 3);
    avro_value_t  *value = lua_avro_get_value(L, 4);

    avro_reader_t  reader = avro_reader_memory(buf, size);
    avro_resolved_writer_set_dest(&l_resolver->value, value);
    int rc = avro_value_read(reader, &l_resolver->value);
    avro_reader_free(reader);

    if (rc != 0) {
        return lua_return_avro_error(L);
    }

    lua_pushboolean(L, true);
    return 1;
}


/*-----------------------------------------------------------------------
 * Lua access — data files
 */

/**
 * The string used to identify the AvroDataInputFile class's metatable
 * in the Lua registry.
 */

#define MT_AVRO_DATA_INPUT_FILE "avro:AvroDataInputFile"


typedef struct _LuaAvroDataInputFile
{
    avro_file_reader_t  reader;
    avro_schema_t  wschema;
    avro_value_iface_t  *iface;
} LuaAvroDataInputFile;

int
lua_avro_push_file_reader(lua_State *L, avro_file_reader_t reader)
{
    LuaAvroDataInputFile  *l_file;

    l_file = lua_newuserdata(L, sizeof(LuaAvroDataInputFile));
    l_file->reader = reader;
    l_file->wschema = avro_file_reader_get_writer_schema(reader);
    l_file->iface = avro_generic_class_from_schema(l_file->wschema);
    luaL_getmetatable(L, MT_AVRO_DATA_INPUT_FILE);
    lua_setmetatable(L, -2);
    return 1;
}


avro_file_reader_t
lua_avro_get_file_reader(lua_State *L, int index)
{
    LuaAvroDataInputFile  *l_file =
        luaL_checkudata(L, index, MT_AVRO_DATA_INPUT_FILE);
    return l_file->reader;
}


/**
 * Closes a file reader.
 */

static int
l_input_file_close(lua_State *L)
{
    LuaAvroDataInputFile  *l_file =
        luaL_checkudata(L, 1, MT_AVRO_DATA_INPUT_FILE);
    if (l_file->reader != NULL) {
        avro_file_reader_close(l_file->reader);
        l_file->reader = NULL;
    }
    l_file->wschema = NULL;
    if (l_file->iface != NULL) {
        avro_value_iface_decref(l_file->iface);
        l_file->iface = NULL;
    }
    return 0;
}

/**
 * Returns the writer schema used to create the file.
 */

static int
l_input_file_schema_json(lua_State *L)
{
    static char  static_buf[65536];
    LuaAvroDataInputFile  *l_file =
        luaL_checkudata(L, 1, MT_AVRO_DATA_INPUT_FILE);

    avro_writer_t  writer = avro_writer_memory(static_buf, sizeof(static_buf));
    int  rc = avro_schema_to_json(l_file->wschema, writer);
    int64_t  length = avro_writer_tell(writer);
    avro_writer_free(writer);

    if (rc != 0) {
        return lua_avro_error(L);
    }

    lua_pushlstring(L, static_buf, length);
    return 1;
}

/**
 * Reads a value from a file reader.
 */

static int
l_input_file_read_raw(lua_State *L)
{
    int  nargs = lua_gettop(L);
    LuaAvroDataInputFile  *l_file =
        luaL_checkudata(L, 1, MT_AVRO_DATA_INPUT_FILE);

    if (nargs == 1) {
        /* No Value instance given, so create one. */
        avro_value_t  value;
        check(avro_generic_value_new(l_file->iface, &value));
        int  rc = avro_file_reader_read_value(l_file->reader, &value);
        if (rc != 0) {
            return lua_return_avro_error(L);
        }
        lua_avro_push_value(L, &value, true);
        return 1;
    }

    else {
        /* Otherwise read into the given value. */
        avro_value_t  *value = lua_avro_get_value(L, 2);
        int  rc = avro_file_reader_read_value(l_file->reader, value);
        if (rc != 0) {
            return lua_return_avro_error(L);
        }
        lua_pushvalue(L, 2);
        return 1;
    }
}


/**
 * The string used to identify the AvroDataOutputFile class's metatable
 * in the Lua registry.
 */

#define MT_AVRO_DATA_OUTPUT_FILE "avro:AvroDataOutputFile"


typedef struct _LuaAvroDataOutputFile
{
    avro_file_writer_t  writer;
} LuaAvroDataOutputFile;


int
lua_avro_push_file_writer(lua_State *L, avro_file_writer_t writer)
{
    LuaAvroDataOutputFile  *l_file;

    l_file = lua_newuserdata(L, sizeof(LuaAvroDataOutputFile));
    l_file->writer = writer;
    luaL_getmetatable(L, MT_AVRO_DATA_OUTPUT_FILE);
    lua_setmetatable(L, -2);
    return 1;
}


avro_file_writer_t
lua_avro_get_file_writer(lua_State *L, int index)
{
    LuaAvroDataOutputFile  *l_file =
        luaL_checkudata(L, index, MT_AVRO_DATA_OUTPUT_FILE);
    return l_file->writer;
}


/**
 * Closes a file writer.
 */

static int
l_output_file_close(lua_State *L)
{
    LuaAvroDataOutputFile  *l_file =
        luaL_checkudata(L, 1, MT_AVRO_DATA_OUTPUT_FILE);
    if (l_file->writer != NULL) {
        avro_file_writer_close(l_file->writer);
        l_file->writer = NULL;
    }
    return 0;
}

/**
 * Writes a value to a file writer.
 */

static int
l_output_file_write(lua_State *L)
{
    avro_file_writer_t  writer = lua_avro_get_file_writer(L, 1);
    avro_value_t  *value = lua_avro_get_value(L, 2);
    check(avro_file_writer_append_value(writer, value));
    return 0;
}


/**
 * Opens a new input or output file.
 */

static int
l_file_open(lua_State *L)
{
    static const char  *MODES[] = { "r", "w", NULL };

    const char  *path = luaL_checkstring(L, 1);
    int  mode = luaL_checkoption(L, 2, "r", MODES);

    if (mode == 0) {
        /* mode == "r" */
        avro_file_reader_t  reader;
        int  rc = avro_file_reader(path, &reader);
        if (rc != 0) {
            return lua_return_avro_error(L);
        }
        lua_avro_push_file_reader(L, reader);
        return 1;

    } else if (mode == 1) {
        /* mode == "w" */
        avro_schema_t  schema = lua_avro_get_schema(L, 3);
        avro_file_writer_t  writer;
        int  rc = avro_file_writer_create(path, schema, &writer);
        if (rc != 0) {
            return lua_return_avro_error(L);
        }
        lua_avro_push_file_writer(L, writer);
        return 1;
    }

    return 0;
}


/*-----------------------------------------------------------------------
 * Lua access — module
 */

static const luaL_Reg  value_methods[] =
{
    {"add", l_value_add},
    {"append", l_value_append},
    {"cmp", l_value_cmp},
    {"copy_from", l_value_copy_from},
    {"discriminant", l_value_discriminant},
    {"discriminant_index", l_value_discriminant_index},
    {"encode", l_value_encode},
    {"encoded_size", l_value_encoded_size},
    {"get", l_value_get},
    {"hash", l_value_hash},
    {"iterate", l_value_iterate},
    {"raw_value", l_value_raw_value},
    {"release", l_value_release},
    {"reset", l_value_reset},
    {"schema_name", l_value_schema_name},
    {"set", l_value_set},
    {"set_dest", l_value_set_dest},
    {"set_from_ast", l_value_set_from_ast},
    {"set_source", l_value_set_source},
    {"size", l_value_size},
    {"to_json", l_value_tostring},
    {"type", l_value_type},
    {NULL, NULL}
};


static const luaL_Reg  schema_methods[] =
{
    {"name", l_schema_name},
    {"new_raw_value", l_schema_new_raw_value},
    {"type", l_schema_type},
    {NULL, NULL}
};


static const luaL_Reg  resolved_reader_methods[] =
{
    {"new_raw_value", l_resolved_reader_new_raw_value},
    {NULL, NULL}
};


static const luaL_Reg  resolved_writer_methods[] =
{
    {"decode", l_resolved_writer_decode},
    {"new_raw_value", l_resolved_writer_new_raw_value},
    {NULL, NULL}
};


static const luaL_Reg  input_file_methods[] =
{
    {"close", l_input_file_close},
    {"read_raw", l_input_file_read_raw},
    {"schema_json", l_input_file_schema_json},
    {NULL, NULL}
};


static const luaL_Reg  output_file_methods[] =
{
    {"close", l_output_file_close},
    {"write_raw", l_output_file_write},
    {NULL, NULL}
};


static const luaL_Reg  mod_methods[] =
{
    {"ResolvedReader", l_resolved_reader_new},
    {"ResolvedWriter", l_resolved_writer_new},
    {"Schema", l_schema_new},
    {"new_raw_schema", l_new_raw_schema},
    {"open", l_file_open},
    {"raw_decode_value", l_value_decode_raw},
    {"raw_encode_value", l_value_encode_raw},
    {NULL, NULL}
};


int
luaopen_avro_legacy_avro(lua_State *L)
{
    /* AvroSchema metatable */

    luaL_newmetatable(L, MT_AVRO_SCHEMA);
    lua_createtable(L, 0, sizeof(schema_methods) / sizeof(luaL_reg) - 1);
    luaL_register(L, NULL, schema_methods);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_schema_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    /* AvroValue metatable */

    luaL_newmetatable(L, MT_AVRO_VALUE);
    lua_createtable(L, 0, sizeof(value_methods) / sizeof(luaL_reg) - 1);
    luaL_register(L, NULL, value_methods);
    lua_setfield(L, -2, "__index");
    lua_pushboolean(L, true);
    lua_setfield(L, -2, "is_raw_value");
    lua_pushcfunction(L, l_value_lt);
    lua_setfield(L, -2, "__lt");
    lua_pushcfunction(L, l_value_le);
    lua_setfield(L, -2, "__le");
    lua_pushcfunction(L, l_value_eq);
    lua_setfield(L, -2, "__eq");
    lua_pushcfunction(L, l_value_tostring);
    lua_setfield(L, -2, "__tostring");
    lua_pop(L, 1);

    luaL_newmetatable(L, MT_ITERATOR);
    lua_pushcfunction(L, iterator_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    /* AvroResolvedReader metatable */

    luaL_newmetatable(L, MT_AVRO_RESOLVED_READER);
    lua_createtable(L, 0, sizeof(resolved_reader_methods) / sizeof(luaL_reg) - 1);
    luaL_register(L, NULL, resolved_reader_methods);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_resolved_reader_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    /* AvroResolvedWriter metatable */

    luaL_newmetatable(L, MT_AVRO_RESOLVED_WRITER);
    lua_createtable(L, 0, sizeof(resolved_writer_methods) / sizeof(luaL_reg) - 1);
    luaL_register(L, NULL, resolved_writer_methods);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_resolved_writer_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    /* AvroInputFile metatable */

    luaL_newmetatable(L, MT_AVRO_DATA_INPUT_FILE);
    lua_createtable(L, 0, sizeof(input_file_methods) / sizeof(luaL_reg) - 1);
    luaL_register(L, NULL, input_file_methods);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_input_file_close);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    /* AvroOutputFile metatable */

    luaL_newmetatable(L, MT_AVRO_DATA_OUTPUT_FILE);
    lua_createtable(L, 0, sizeof(output_file_methods) / sizeof(luaL_reg) - 1);
    luaL_register(L, NULL, output_file_methods);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, l_output_file_close);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    luaL_register(L, "avro.legacy.avro", mod_methods);
    return 1;
}
