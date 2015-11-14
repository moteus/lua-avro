-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011-2015, RedJack, LLC.
-- All rights reserved.
--
-- Please see the COPYING file in this distribution for license details.
------------------------------------------------------------------------

local A = require "avro"

------------------------------------------------------------------------
-- Helpers

-- The following function is from [1], and is MIT/X11-licensed.
-- [1] http://snippets.luacode.org/snippets/Deep_Comparison_of_Two_Values_3

function deepcompare(t1,t2,ignore_mt)
   local ty1 = type(t1)
   local ty2 = type(t2)
   if ty1 ~= ty2 then return false end
   -- non-table types can be directly compared
   if ty1 ~= 'table' and ty2 ~= 'table' then return t1 == t2 end
   -- as well as tables which have the metamethod __eq
   local mt = getmetatable(t1)
   if not ignore_mt and mt and mt.__eq then return t1 == t2 end
   for k1,v1 in pairs(t1) do
      local v2 = t2[k1]
      if v2 == nil or not deepcompare(v1,v2) then return false end
   end
   for k2,v2 in pairs(t2) do
      local v1 = t1[k2]
      if v1 == nil or not deepcompare(v1,v2) then return false end
   end
   return true
end

------------------------------------------------------------------------
-- Schema:type()

do
   local function test_parse(json, expected)
      local schema = A.Schema:new(json)
      local actual = schema:type()
      assert(actual == expected)
   end

   local function test_prim(prim_type, expected)
      test_parse([[{"type": "]]..prim_type..[["}]], expected)
   end

   test_prim("boolean", A.BOOLEAN)
   test_prim("bytes", A.BYTES)
   test_prim("double", A.DOUBLE)
   test_prim("float", A.FLOAT)
   test_prim("int", A.INT)
   test_prim("long", A.LONG)
   test_prim("null", A.NULL)
   test_prim("string", A.STRING)
end

------------------------------------------------------------------------
-- Arrays

do
   local function test_array(prim_type, expected)
      local items_schema = A.Schema:new([[{"type": "]]..prim_type..[["}]])
      local schema = A.ArraySchema:new(items_schema)
      local _, array = schema:new_wrapped_value()
      for _,val in ipairs(expected) do
         array:append(val)
      end
      local _, array2 = schema:new_wrapped_value()
      array2:copy_from(array)
      local actual = {}
      for _,element in array:iterate() do
         table.insert(actual, element)
      end
      local _, array3 = schema:new_wrapped_value()
      array3:set_from_ast(expected)
      assert(deepcompare(actual, expected))
      assert(array == array2)
      assert(array == array3)
      assert(array:hash() == array2:hash())
      for i,e in array:iterate(true) do
         assert(e:get() == expected[i])
      end
      array:release()
      array2:release()
      array3:release()
   end

   test_array("int", { 1,2,3,4 })
   test_array("string", { "", "a", "hello", "world!" })
end

------------------------------------------------------------------------
-- Maps

do
   local function test_map(prim_type, expected)
      local schema = A.Schema:new([[{"type": "map", "values": "]]..prim_type..[["}]])
      local _, map = schema:new_wrapped_value()
      for key,val in pairs(expected) do
         map:set(key, val)
      end
      local _, map2 = schema:new_wrapped_value()
      map2:copy_from(map)
      local actual = {}
      for key,element in map:iterate() do
         actual[key] = element
      end
      local _, map3 = schema:new_wrapped_value()
      map3:set_from_ast(expected)
      assert(deepcompare(actual, expected))
      assert(map == map2)
      assert(map == map3)
      assert(map:hash() == map2:hash())
      for k,e in map:iterate(true) do
         assert(e:get() == expected[k])
      end
      map:release()
      map2:release()
      map3:release()
   end

   test_map("int", { a=1,b=2,c=3,d=4 })
   test_map("string", { a="", b="a", c="hello", d="world!" })
end

------------------------------------------------------------------------
-- Records

do
   local schema = A.Schema:new [[
      {
         "type": "record",
         "name": "test",
         "fields": [
            { "name": "i", "type": "int" },
            { "name": "b", "type": "boolean" },
            { "name": "s", "type": "string" },
            { "name": "ls", "type": { "type": "array", "items": "long" } }
         ]
      }
   ]]

   local _, rec = schema:new_wrapped_value()
   rec.i = 1
   rec.b = true
   rec.s = "fantastic"
   rec.ls:append(1)
   rec.ls:append(100)

   local _, rec2 = schema:new_wrapped_value()
   rec2:copy_from(rec)

   local _, rec3 = schema:new_wrapped_value()
   rec3:set_from_ast {
      i = 1,
      b = true,
      s = "fantastic",
      ls = { 1, 100 },
   }

   assert(rec == rec2)
   assert(rec == rec3)

   rec:release()
   rec2:release()
   rec3:release()
end

------------------------------------------------------------------------
-- Unions

do
   local schema = A.Schema:new [[
      [
         "null", "int",
         { "type": "record", "name": "test",
           "fields": [ {"name": "a", "type": "int" } ] }
      ]
   ]]

   local _, union = schema:new_wrapped_value()
   local _, union2 = schema:new_wrapped_value()
   local _, union3 = schema:new_wrapped_value()

   union.null = nil
   union2:copy_from(union)
   union3:set_from_ast(nil)
   assert(union == union2)
   assert(union == union3)

   union.int = 42
   union2:copy_from(union)
   union3:set_from_ast { int = 42 }
   assert(union == union2)
   assert(union == union3)

   union.test.a = 10
   union2:copy_from(union)
   union3:set_from_ast { test = { a = 10 } }
   assert(union == union2)
   assert(union == union3)

   union:release()
   union2:release()
   union3:release()
end

------------------------------------------------------------------------
-- ResolvedReader()

do
   local function test_good_scalar(json1, json2, scalar)
      local schema1 = A.Schema:new([[{"type": "]]..json1..[["}]])
      local schema2 = A.Schema:new([[{"type": "]]..json2..[["}]])
      local resolver = assert(A.ResolvedReader(schema1, schema2))

      local raw_value = schema1:new_raw_value()
      local raw_resolved = resolver:new_raw_value()
      raw_resolved:set_source(raw_value)

      raw_value:set(scalar)
      local wrapper_class = schema2:wrapper_class()
      local wrapper = wrapper_class:new()
      local wrapped_resolved = wrapper:wrap(raw_resolved)
      assert(wrapped_resolved == scalar)
      raw_value:release()
      raw_resolved:release()
   end

   test_good_scalar("int", "int", 42)
   test_good_scalar("int", "long", 42)

   local schema1 = A.Schema:new [[
     {
       "type": "record",
       "name": "foo",
       "fields": [
         {"name": "a", "type": "int"},
         {"name": "b", "type": "double"}
       ]
     }
   ]]

   local schema2 = A.Schema:new [[
     {
       "type": "record",
       "name": "foo",
       "fields": [
         {"name": "a", "type": "int"}
       ]
     }
   ]]

   local resolver = assert(A.ResolvedReader(schema1, schema2))

   local _, val1 = schema1:new_wrapped_value()
   val1.a = 1
   val1.b = 42

   local _, val2 = schema1:new_wrapped_value()
   val2.a = 1
   val2.b = 100

   local resolved1 = resolver:new_raw_value()
   resolved1:set_source(val1.raw)

   local resolved2 = resolver:new_raw_value()
   resolved2:set_source(val2.raw)

   assert(val1 ~= val2)
   assert(resolved1 == resolved2)

   val1:release()
   val2:release()
   resolved1:release()
   resolved2:release()
end

------------------------------------------------------------------------
-- ResolvedWriter()

do
   local function test_good_resolver(json1, json2)
      local schema1 = A.Schema:new(json1)
      local schema2 = A.Schema:new(json2)
      local resolver = assert(A.ResolvedWriter(schema1, schema2))
   end

   local function test_good_prim(prim_type1, prim_type2)
      test_good_resolver([[{"type": "]]..prim_type1..[["}]],
                         [[{"type": "]]..prim_type2..[["}]])
   end

   local function test_bad_resolver(json1, json2)
      local schema1 = A.Schema:new(json1)
      local schema2 = A.Schema:new(json2)
      local resolver = assert(not A.ResolvedWriter(schema1, schema2))
   end

   local function test_bad_prim(prim_type1, prim_type2)
      test_bad_resolver([[{"type": "]]..prim_type1..[["}]],
                        [[{"type": "]]..prim_type2..[["}]])
   end

   test_good_prim("boolean", "boolean")
   test_bad_prim ("boolean", "bytes")

   test_good_prim("bytes", "bytes")
   test_bad_prim ("bytes", "double")

   test_good_prim("double", "double")
   test_bad_prim ("double", "int")

   test_good_prim("float", "float")
   test_good_prim("float", "double")
   test_bad_prim ("float", "int")

   test_good_prim("int", "int")
   test_good_prim("int", "long")
   test_good_prim("int", "float")
   test_good_prim("int", "double")
   test_bad_prim ("int", "null")

   test_good_prim("long", "long")
   test_good_prim("long", "float")
   test_good_prim("long", "double")
   test_bad_prim ("long", "null")

   test_good_prim("null", "null")
   test_bad_prim ("null", "string")

   test_good_prim("string", "string")
   test_bad_prim ("string", "boolean")
end

------------------------------------------------------------------------
-- Resolver:decode()

do
   local function test_boolean(buf, expected_prim)
      local schema = A.Schema:new([[{"type": "boolean"}]])
      local raw_actual = schema:new_raw_value()
      local resolver = assert(A.ResolvedWriter(schema, schema))
      assert(resolver:decode(buf, raw_actual))
      local wrapper_class = schema:wrapper_class()
      local wrapper = wrapper_class:new()
      local actual = wrapper:wrap(raw_actual)
      assert(actual == expected_prim)
      raw_actual:release()
   end

   test_boolean("\000", false)
   test_boolean("\001", true)

   local function test_int(buf, expected_prim)
      local schema = A.Schema:new([[{"type": "int"}]])
      local raw_actual = schema:new_raw_value()
      local resolver = assert(A.ResolvedWriter(schema, schema))
      assert(resolver:decode(buf, raw_actual))
      local wrapper_class = schema:wrapper_class()
      local wrapper = wrapper_class:new()
      local actual = wrapper:wrap(raw_actual)
      assert(actual == expected_prim)
      raw_actual:release()
   end

   test_int("\000", 0)
   test_int("\001", -1)
   test_int("\002", 1)
end

------------------------------------------------------------------------
-- Resolver:encode()

do
   local function test_boolean(expected_buf, prim_value)
      local schema = A.Schema:new([[{"type": "boolean"}]])
      local raw_value = schema:new_raw_value()
      local wrapper_class = schema:wrapper_class()
      local wrapper = wrapper_class:new()
      wrapper:wrap(raw_value)
      wrapper:fill_from(prim_value)
      local actual_buf = assert(raw_value:encode())
      assert(actual_buf == expected_buf)
      raw_value:release()
   end

   test_boolean("\000", false)
   test_boolean("\001", true)

   local function test_int(expected_buf, prim_value)
      local schema = A.Schema:new([[{"type": "int"}]])
      local raw_value = schema:new_raw_value()
      local wrapper_class = schema:wrapper_class()
      local wrapper = wrapper_class:new()
      wrapper:wrap(raw_value)
      wrapper:fill_from(prim_value)
      local actual_buf = assert(raw_value:encode())
      assert(actual_buf == expected_buf)
      raw_value:release()
   end

   test_int("\000", 0)
   test_int("\001", -1)
   test_int("\002", 1)
end

------------------------------------------------------------------------
-- Files

do
   local expected = {1,2,3,4,5,6,7,8,9,10}

   local filename = "test-data.avro"
   local schema = A.Schema:new([[{"type": "int"}]])
   local writer = A.open(filename, "w", schema)
   local raw_value = schema:new_raw_value()

   for _,i in ipairs(expected) do
      raw_value:set(i)
      writer:write_raw(raw_value)
   end

   writer:close()
   raw_value:release()

   local reader, actual

   -- Read once passing in a value parameter, once without.

   reader = A.open(filename)
   actual = {}
   raw_value = reader:read_raw()
   while raw_value do
      local wrapper_class = schema:wrapper_class()
      local wrapper = wrapper_class:new()
      local value = wrapper:wrap(raw_value)
      table.insert(actual, value)
      raw_value:release()
      raw_value = reader:read_raw()
   end
   reader:close()
   assert(deepcompare(expected, actual))

   reader = A.open(filename)
   actual = {}
   raw_value = schema:new_raw_value()
   local ok = reader:read_raw(raw_value)
   while ok do
      local wrapper_class = schema:wrapper_class()
      local wrapper = wrapper_class:new()
      local value = wrapper:wrap(raw_value)
      table.insert(actual, value)
      ok = reader:read_raw(raw_value)
   end
   reader:close()
   raw_value:release()
   assert(deepcompare(expected, actual))

   -- And cleanup
   os.remove(filename)
end

------------------------------------------------------------------------
-- Recursive

do
   local schema = A.record "list" {
      {head = A.long},
      {tail = A.union {A.null, A.link "list"}},
   }

   local raw0 = schema:new_raw_value()
   raw0:get("head"):set(0)
   raw0:get("tail"):set("list"):get("head"):set(1)
   raw0:get("tail"):get():get("tail"):set("null")

   local raw1 = schema:new_raw_value()
   local wrapper_class = schema:wrapper_class()
   local wrapper = wrapper_class:new()
   local wrap1 = wrapper:wrap(raw1)
   wrap1.head = 0
   wrap1.tail.list.head = 1
   wrap1.tail.list.tail.null = nil

   assert(raw0 == raw1)

   raw0:release()
   raw1:release()
end
