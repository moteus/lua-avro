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
-- Arrays

do
   local function test_array(prim_type, expected)
      local items_schema = A.Schema:new([[{"type": "]]..prim_type..[["}]])
      local schema = A.ArraySchema:new(items_schema)
      local array = schema:new_raw_value()
      for _,val in ipairs(expected) do
         local element = array:append()
         element:set(val)
      end
      local array2 = schema:new_raw_value()
      array2:copy_from(array)
      local actual = {}
      for _,element in array:iterate() do
         table.insert(actual, element:get())
      end
      local array3 = schema:new_raw_value()
      array3:set_from_ast(expected)
      assert(deepcompare(actual, expected))
      assert(array == array2)
      assert(array == array3)
      assert(array:hash() == array2:hash())
      for i,e in array:iterate(true) do
         assert(e:get() == expected[i])
      end
      assert(array:get(1):get() == expected[1])
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
      local map = schema:new_raw_value()
      local first_key
      for key,val in pairs(expected) do
         if not first_key then first_key = key end
         local element = map:set(key)
         element:set(val)
      end
      local map2 = schema:new_raw_value()
      map2:copy_from(map)
      local actual = {}
      for key,element in map:iterate() do
         actual[key] = element:get()
      end
      local map3 = schema:new_raw_value()
      map3:set_from_ast(expected)
      assert(deepcompare(actual, expected))
      assert(map == map2)
      assert(map == map3)
      assert(map:hash() == map2:hash())
      assert(map:get(1):get() == expected[first_key])
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
-- set_from_ast conversions

do
   function test(schema, ast, raw)
      local value1 = schema:new_raw_value()
      local value2 = schema:new_raw_value()
      value1:set_from_ast(ast)
      value2:set(raw)
      assert(value1 == value2)
      value1:release()
      value2:release()
   end

   test(A.string, 12, "12")
   test(A.string, "12", "12")
   test(A.int, "12", 12)
   test(A.int, 12, 12)
end

------------------------------------------------------------------------
-- Enums

do
   local schema = A.enum "colors" { "RED", "GREEN", "BLUE" }
   local value = schema:new_raw_value()
   value:set("RED")
   assert(value:get() == "RED")
   value:set(2)
   assert(value:get() == "GREEN")
   value:release()
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

   local rec = schema:new_raw_value()
   rec:get("i"):set(1)
   rec:get(2):set(true)
   rec:get("s"):set("fantastic")
   rec:get(4):append():set(1)
   rec:get("ls"):append():set(100)

   local rec2 = schema:new_raw_value()
   rec2:copy_from(rec)

   local rec3 = schema:new_raw_value()
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

   local union = schema:new_raw_value()
   local union2 = schema:new_raw_value()
   local union3 = schema:new_raw_value()

   union:set("null")
   assert(union:discriminant_index() == 1)
   assert(union:discriminant() == "null")
   union2:copy_from(union)
   union3:set_from_ast(nil)
   assert(union == union2)
   assert(union == union3)

   union:get("int"):set(42)
   assert(union:discriminant_index() == 2)
   assert(union:discriminant() == "int")
   union2:copy_from(union)
   union3:set_from_ast { int = 42 }
   assert(union == union2)
   assert(union == union3)

   union:set(3)
   assert(union:discriminant_index() == 3)
   assert(union:discriminant() == "test")
   union:get():get("a"):set(10)
   union2:copy_from(union)
   union3:set_from_ast { test = { a = 10 } }
   assert(union == union2)
   assert(union == union3)

   union:release()
   union2:release()
   union3:release()
end

------------------------------------------------------------------------
-- Sort order

do
   local function test(schema, ast1, ast2)
      local val1 = schema:new_raw_value()
      local val2 = schema:new_raw_value()

      val1:set_from_ast(ast1)
      val2:set_from_ast(ast2)

      assert(val1 < val2)
      assert(val2 > val1)

      assert(val1 <= val1)
      assert(val1 >= val1)
      assert(val2 <= val2)
      assert(val2 >= val2)

      assert(not (val1 < val1))
      assert(not (val1 > val1))

      assert(not (val2 < val2))
      assert(not (val2 > val2))

      val1:release()
      val2:release()
   end

   test(A.boolean, false, true)
   test(A.double, -42.53, 72.12)
   test(A.float, -42.53, 72.12)
   test(A.int, -10, 42)
   test(A.long, -10, 42)

   test(A.array { A.int }, {}, {12})
   test(A.array { A.int }, {11}, {12})
end

------------------------------------------------------------------------
-- ResolvedReader()

do
   local function test_good_scalar(json1, json2, scalar)
      local schema1 = A.Schema:new([[{"type": "]]..json1..[["}]])
      local schema2 = A.Schema:new([[{"type": "]]..json2..[["}]])
      local resolver = assert(A.ResolvedReader(schema1, schema2))

      local value = schema1:new_raw_value()
      local resolved = resolver:new_raw_value()
      resolved:set_source(value)

      value:set(scalar)
      assert(resolved:get() == scalar)
      value:release()
      resolved:release()
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

   local val1 = schema1:new_raw_value()
   val1:get("a"):set(1)
   val1:get("b"):set(42)

   local val2 = schema1:new_raw_value()
   val2:get(1):set(1)
   val2:get(2):set(100)

   local resolved1 = resolver:new_raw_value()
   resolved1:set_source(val1)

   local resolved2 = resolver:new_raw_value()
   resolved2:set_source(val2)

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
      local actual = schema:new_raw_value()
      local resolver = assert(A.ResolvedWriter(schema, schema))
      assert(resolver:decode(buf, actual))
      assert(actual:get() == expected_prim)
      actual:release()
   end

   test_boolean("\000", false)
   test_boolean("\001", true)

   local function test_int(buf, expected_prim)
      local schema = A.Schema:new([[{"type": "int"}]])
      local actual = schema:new_raw_value()
      local resolver = assert(A.ResolvedWriter(schema, schema))
      assert(resolver:decode(buf, actual))
      assert(actual:get() == expected_prim)
      actual:release()
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
      local value = schema:new_raw_value()
      value:set(prim_value)
      local actual_buf = assert(value:encode())
      assert(actual_buf == expected_buf)
      value:release()
   end

   test_boolean("\000", false)
   test_boolean("\001", true)

   local function test_int(expected_buf, prim_value)
      local schema = A.Schema:new([[{"type": "int"}]])
      local actual = schema:new_raw_value()
      local value = schema:new_raw_value()
      value:set(prim_value)
      local actual_buf = assert(value:encode())
      assert(actual_buf == expected_buf)
      actual:release()
      value:release()
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
   local value = schema:new_raw_value()

   for _,i in ipairs(expected) do
      value:set(i)
      writer:write_raw(value)
   end

   writer:close()
   value:release()

   local reader, actual

   -- Read once passing in a value parameter, once without.

   reader = A.open(filename)
   actual = {}
   value = reader:read_raw()
   while value do
      table.insert(actual, value:get())
      value:release()
      value = reader:read_raw()
   end
   reader:close()
   assert(deepcompare(expected, actual))

   reader = A.open(filename)
   actual = {}
   value = schema:new_raw_value()
   local ok = reader:read_raw(value)
   while ok do
      table.insert(actual, value:get())
      ok = reader:read_raw(value)
   end
   reader:close()
   value:release()
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

   local raw_value = schema:new_raw_value()
   raw_value:get("head"):set(0)
   raw_value:get("tail"):set("list"):get("head"):set(1)
   raw_value:get("tail"):get():get("tail"):set("null")
   raw_value:release()
end
