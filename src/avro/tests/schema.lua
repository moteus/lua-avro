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

      local clone = schema:clone()
      assert(schema == clone)
   end

   local function test_prim(prim_type, expected)
      test_parse([[{"type": "]]..prim_type..[["}]], expected)
      test_parse([["]]..prim_type..[["]], expected)
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
-- Schema:fields()

do
   local r = A.record "test" {
      {i = A.int},
      {l = A.long},
      {b = A.boolean},
   }

   assert(deepcompare(r.fields, {
      {i = A.int},
      {l = A.long},
      {b = A.boolean},
   }))
end

------------------------------------------------------------------------
-- Helper constructors

do
   local json = [[
      {
         "type": "record",
         "name": "test",
         "fields": [
            {"name": "i", "type": "int"},
            {"name": "l", "type": "long"},
            {"name": "e", "type":
               {
                  "type": "enum",
                  "name": "color",
                  "symbols": ["RED","GREEN","BLUE"]
               }
            },
            {"name": "a", "type":
               { "type": "array", "items": "double" }},
            {"name": "m", "type":
               { "type": "map", "values": "float" }},
            {"name": "ipv4", "type":
               { "type": "fixed", "name": "ipv4", "size": 4 }},
            {"name": "dest_ipv4", "type": "ipv4"},
            {"name": "ipv6", "type":
               { "type": "fixed", "name": "ipv6", "size": 16 }},
            {"name": "u", "type": ["null", "string", "double"]},
            {"name": "sub", "type":
               {
                  "type": "record",
                  "name": "subtest",
                  "fields": [
                     {"name": "s", "type": "string"}
                  ]
               }
            },
            {"name": "children", "type":
               { "type": "array", "items": "test" }},
            {"name": "parent", "type": ["null", "test"]}
         ]
      }
   ]]

   local schema1 = A.Schema:new(json)
   local schema2 = A.record "test" {
      {i = A.int},
      {l = A.Schema:new([[ {"type": "long"} ]])},
      {e = A.enum "color" {"RED","GREEN","BLUE"} },
      {a = A.array { A.double }},
      {m = A.map(A.float)},
      {ipv4 = A.fixed "ipv4" {size=4}},
      {dest_ipv4 = A.fixed "ipv4" {size=4}},
      {ipv6 = A.fixed "ipv6"(16)},
      {u = A.union {A.null, A.string, A.double}},
      {sub = A.record "subtest" {
         s = A.string,
      }},
      {children = A.array { A.link "test" }},
      {parent = A.union { A.null, A.link "test" }},
   }
   local schema3 = A.array { A.double }

   --print(schema1)
   --print(schema2)
   assert(schema1 == schema2)
   assert(schema1:size() == 12)
   assert(schema2:get("a") == schema3)

   assert(deepcompare(schema1:field_names(), {
      "i", "l", "e", "a", "m", "ipv4", "dest_ipv4", "ipv6",
      "u", "sub", "children", "parent",
   }))

   local clone = schema1:clone()
   assert(schema1 == clone)
   assert(schema2 == clone)
end
