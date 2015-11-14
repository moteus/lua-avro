-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011-2015, RedJack, LLC.
-- All rights reserved.
--
-- Please see the COPYING file in this distribution for license details.
------------------------------------------------------------------------

local AC = require "avro.c"
local ACC = require "avro.constants"
local json = require "avro.dkjson"
local AW = require "avro.wrapper"

local assert = assert
local error = error
local getmetatable = getmetatable
local ipairs = ipairs
local next = next
local pairs = pairs
local print = print
local setmetatable = setmetatable
local table = table
local tonumber = tonumber
local tostring = tostring
local type = type

module "avro.schema"

------------------------------------------------------------------------
-- Base schema class

Schema = {}
Schema.__mt = { __index=Schema }

function Schema:to_json()
   if not self.json then
      self.json = self:build_json({})
   end
   return self.json
end

Schema.__mt.__tostring = Schema.to_json

function Schema:check_for_existing(link_table)
   -- Verify that there's not already a schema with the same name in the
   -- link table, unless it's equivalent to ourselves.
   local existing = link_table[self.schema_name]
   if existing then
      if self == existing then
         -- The existing schema is the same as self, so the JSON
         -- encoding of ourselves is a link to the existing schema.
         return [["]]..self.schema_name..[["]]
      else
         error("Two mismatching schemas named "..self.schema_name)
      end
   end

   link_table[self.schema_name] = self
   return nil
end

function Schema.__mt:__eq(other)
   if other == nil then return false end
   return self:to_json() == other:to_json()
end

function Schema:raw_schema()
   if not self.raw then
      self.raw = assert(AC.Schema(self:to_json()))
   end
   return self.raw
end

function Schema:new_raw_value(...)
   local raw = self:raw_schema()
   return raw:new_raw_value(...)
end

function Schema:new_wrapped_value()
   local raw = self:new_raw_value()
   local wrapper_class = self:wrapper_class()
   local wrapper = wrapper_class:new()
   return wrapper, wrapper:wrap(raw)
end

function Schema:name()
   return self.schema_name
end

function Schema:type()
   return self.schema_type
end

function Schema:size()
   error("Can only get the size of a fixed, record, or union schema")
end

function Schema:wrapper_class()
   if not self.__wrapper_class then
      self.__wrapper_class = AW.get_wrapper_class(self.schema_name)
      if not self.__wrapper_class then
         self.__wrapper_class = self:default_wrapper_class()
      end
   end
   return self.__wrapper_class
end

function Schema:default_wrapper_class()
   error("Don't have a default wrapper for this schema")
end


------------------------------------------------------------------------
-- Primitive schema class

local PrimitiveSchema = {}
PrimitiveSchema.__mt = { __index=PrimitiveSchema }
setmetatable(PrimitiveSchema, { __index=Schema })

function PrimitiveSchema:new(name, schema_type, default_wrapper_class)
   local obj = {
      schema_name=name,
      schema_type=schema_type,
      __default_wrapper_class=default_wrapper_class,
   }
   return setmetatable(obj, self.__mt)
end

PrimitiveSchema.__mt.__tostring = Schema.__mt.__tostring
PrimitiveSchema.__mt.__eq = Schema.__mt.__eq

function PrimitiveSchema:build_json(link_table)
   return [[{"type": "]]..self.schema_name..[["}]]
end

function PrimitiveSchema:default_wrapper_class()
   return self.__default_wrapper_class
end

function PrimitiveSchema:clone(clones)
   return self
end


local PRIMITIVES = {}

local function primitive_schema(name, schema_type, default_wrapper_class)
   local schema = PrimitiveSchema:new(name, schema_type, default_wrapper_class)
   PRIMITIVES[name] = schema
   return schema
end

boolean = primitive_schema("boolean", ACC.BOOLEAN, AW.ScalarValue)
bytes = primitive_schema("bytes", ACC.BYTES, AW.StringValue)
double = primitive_schema("double", ACC.DOUBLE, AW.ScalarValue)
float = primitive_schema("float", ACC.FLOAT, AW.ScalarValue)
int = primitive_schema("int", ACC.INT, AW.ScalarValue)
long = primitive_schema("long", ACC.LONG, AW.LongValue)
null = primitive_schema("null", ACC.NULL, AW.ScalarValue)
string = primitive_schema("string", ACC.STRING, AW.StringValue)


------------------------------------------------------------------------
-- Arrays and maps

ArraySchema = {}
ArraySchema.__mt = { __index=ArraySchema }
setmetatable(ArraySchema, { __index=Schema })

function ArraySchema:new(item_schema)
   local obj = {
      schema_name="array",
      schema_type=ACC.ARRAY,
      item_schema=item_schema,
   }
   return setmetatable(obj, self.__mt)
end

ArraySchema.__mt.__tostring = Schema.__mt.__tostring
ArraySchema.__mt.__eq = Schema.__mt.__eq

function ArraySchema:build_json(link_table)
   local item_json = self.item_schema:build_json(link_table)
   return [[{"type": "array", "items": ]]..item_json..[[}]]
end

function ArraySchema:default_wrapper_class()
   local class = AW.ArrayValue:subclass(self.schema_name)
   self.__wrapper_class = class
   local child_schema = self.item_schema
   local child_class = assert(child_schema:wrapper_class())
   class.__child_class = child_class
   return class
end

function ArraySchema:clone(clones)
   local item_clone = self.item_schema:clone(clones or {})
   return ArraySchema:new(item_clone)
end


MapSchema = {}
MapSchema.__mt = { __index=MapSchema }
setmetatable(MapSchema, { __index=Schema })

function MapSchema:new(value_schema)
   local obj = {
      schema_name="map",
      schema_type=ACC.MAP,
      value_schema=value_schema,
   }
   return setmetatable(obj, self.__mt)
end

MapSchema.__mt.__tostring = Schema.__mt.__tostring
MapSchema.__mt.__eq = Schema.__mt.__eq

function MapSchema:build_json(link_table)
   local value_json = self.value_schema:build_json(link_table)
   return [[{"type": "map", "values": ]]..value_json..[[}]]
end

function MapSchema:default_wrapper_class()
   local class = AW.MapValue:subclass(self.schema_name)
   self.__wrapper_class = class
   local child_schema = self.value_schema
   local child_class = assert(child_schema:wrapper_class())
   class.__child_class = child_class
   return class
end

function MapSchema:clone()
   local value_clone = self.value_schema:clone(clones or {})
   return MapSchema:new(value_clone)
end


------------------------------------------------------------------------
-- Enums

EnumSchema = {}
EnumSchema.__mt = { __index=EnumSchema }
setmetatable(EnumSchema, { __index=Schema })

function EnumSchema:new(name, symbols)
   local obj = {
      schema_name=name,
      schema_type=ACC.ENUM,
      symbols=symbols or {},
   }
   return setmetatable(obj, self.__mt)
end

EnumSchema.__mt.__tostring = Schema.__mt.__tostring
EnumSchema.__mt.__eq = Schema.__mt.__eq

function EnumSchema:size()
   return #self.symbols
end

function EnumSchema:add_symbol(symbol)
   table.insert(self.symbols, symbol)
   self.json = nil
   self.raw = nil
end

function EnumSchema:build_json(link_table)
   local existing = self:check_for_existing(link_table)
   if existing then return existing end

   local symbol_strs = {}
   for _, sym in ipairs(self.symbols) do
      table.insert(symbol_strs, [["]]..sym..[["]])
   end
   local all_symbols = table.concat(symbol_strs, ",")

   return [[{"type": "enum", "name": "]]..self.schema_name..
          [[", "symbols": []]..all_symbols.."]}"
end

function EnumSchema:default_wrapper_class()
   return AW.ScalarValue
end

function EnumSchema:clone(clones)
   clones = clones or {}
   if clones[self.schema_name] then
      return clones[self.schema_name]
   end

   local schema = EnumSchema:new(self.schema_name)
   clones[self.schema_name] = schema
   for _, sym in ipairs(self.symbols) do
      schema:add_symbol(sym)
   end
   return schema
end


------------------------------------------------------------------------
-- Fixeds

FixedSchema = {}
FixedSchema.__mt = { __index=FixedSchema }
setmetatable(FixedSchema, { __index=Schema })

function FixedSchema:new(name, size)
   local obj = {
      schema_name=name,
      schema_type=ACC.FIXED,
      fixed_size=size,
   }
   return setmetatable(obj, self.__mt)
end

FixedSchema.__mt.__tostring = Schema.__mt.__tostring
FixedSchema.__mt.__eq = Schema.__mt.__eq

function FixedSchema:build_json(link_table)
   local existing = self:check_for_existing(link_table)
   if existing then return existing end
   return [[{"type": "fixed", "name": "]]..self.schema_name..
          [[", "size": ]]..self.fixed_size..[[}]]
end

function FixedSchema:default_wrapper_class()
   return AW.ScalarValue
end

function FixedSchema:clone(clones)
   clones = clones or {}
   if clones[self.schema_name] then
      return clones[self.schema_name]
   end

   local schema = FixedSchema:new(self.schema_name, self.fixed_size)
   clones[self.schema_name] = schema
   return schema
end


------------------------------------------------------------------------
-- Records

RecordSchema = {}
RecordSchema.__mt = { __index=RecordSchema }
setmetatable(RecordSchema, { __index=Schema })

function RecordSchema:new(name)
   local obj = {
      schema_name=name,
      schema_type=ACC.RECORD,
      fields={},
      fields_by_name={},
   }
   return setmetatable(obj, self.__mt)
end

RecordSchema.__mt.__tostring = Schema.__mt.__tostring
RecordSchema.__mt.__eq = Schema.__mt.__eq

function RecordSchema:size()
   return #self.fields
end

function RecordSchema:get(field_name)
   return self.fields_by_name[field_name]
end

function RecordSchema:add_field(name, schema)
   table.insert(self.fields, {[name]=schema})
   self.fields_by_name[name] = schema
   self.json = nil
   self.raw = nil
end

function RecordSchema:build_json(link_table)
   local existing = self:check_for_existing(link_table)
   if existing then return existing end

   local field_strs = {}
   for _, field in ipairs(self.fields) do
      local field_name, field_schema = next(field)
      local field_schema_str = field_schema:build_json(link_table)
      table.insert(field_strs,
                   [[{"name": "]]..field_name..
                   [[", "type": ]]..field_schema_str..[[}]])
   end
   local all_fields = table.concat(field_strs, ", ")

   return [[{"type": "record", "name": "]]..self.schema_name..
          [[", "fields": []]..all_fields.."]}"
end

function RecordSchema:default_wrapper_class()
   local class = AW.RecordValue:subclass(self.schema_name)
   self.__wrapper_class = class

   local child_classes = {}
   local real_indices = {}
   for i, field in ipairs(self.fields) do
      local field_name, field_schema = next(field)
      local child_class = assert(field_schema:wrapper_class())
      child_classes[i] = child_class
      child_classes[field_name] = child_class
      real_indices[i] = i
      real_indices[field_name] = i
   end

   class.__child_classes = child_classes
   class.__real_indices = real_indices
   class.__field_names = self:field_names()
   return class
end

function RecordSchema:clone(clones)
   clones = clones or {}
   if clones[self.schema_name] then
      return clones[self.schema_name]
   end

   local schema = RecordSchema:new(self.schema_name)
   clones[self.schema_name] = schema
   for _, field in ipairs(self.fields) do
      local field_name, field_schema = next(field)
      local field_clone = field_schema:clone(clones)
      schema:add_field(field_name, field_clone)
   end
   return schema
end

function RecordSchema:field_names()
   local result = {}
   for _, field in ipairs(self.fields) do
      local field_name, _ = next(field)
      table.insert(result, field_name)
   end
   return result
end


------------------------------------------------------------------------
-- Unions

UnionSchema = {}
UnionSchema.__mt = { __index=UnionSchema }
setmetatable(UnionSchema, { __index=Schema })

function UnionSchema:new()
   local obj = {
      schema_name="union",
      schema_type=ACC.UNION,
      branches={},
      indices_by_name={},
   }
   return setmetatable(obj, self.__mt)
end

UnionSchema.__mt.__tostring = Schema.__mt.__tostring
UnionSchema.__mt.__eq = Schema.__mt.__eq

function UnionSchema:size()
   return #self.branches
end

function UnionSchema:add_branch(branch_schema)
   if branch_schema:type() == ACC.UNION then
      error("Can't add a union to a union")
   end

   local branch_name = branch_schema:name()
   if self.indices_by_name[branch_name] then
      error("Already have a branch named "..branch_name)
   end

   table.insert(self.branches, branch_schema)
   self.indices_by_name[branch_name] = #self.branches
   self.json = nil
   self.raw = nil
end

function UnionSchema:build_json(link_table)
   local branch_strs = {}
   for _, branch_schema in ipairs(self.branches) do
      local branch_schema_str = branch_schema:build_json(link_table)
      table.insert(branch_strs, branch_schema_str)
   end
   local all_branches = table.concat(branch_strs, ", ")

   return "["..all_branches.."]"
end

function UnionSchema:default_wrapper_class()
   local class = AW.UnionValue:subclass(self.schema_name)
   self.__wrapper_class = class

   local child_classes = {}
   local real_indices = {}
   for i, branch_schema in ipairs(self.branches) do
      local branch_name = branch_schema:name()
      local child_class = assert(branch_schema:wrapper_class())
      child_classes[i] = child_class
      child_classes[branch_name] = child_class
      real_indices[i] = i
      real_indices[branch_name] = i
   end

   class.__child_classes = child_classes
   class.__real_indices = real_indices
   return class
end

function UnionSchema:clone(clones)
   clones = clones or {}
   local schema = UnionSchema:new(self.schema_name)
   for _, branch_schema in ipairs(self.branches) do
      local branch_clone = branch_schema:clone(clones)
      schema:add_branch(branch_clone)
   end
   return schema
end


------------------------------------------------------------------------
-- Construct a schema from JSON

local function parse_decoded_json(decoded, link_table)
   if type(decoded) == "string" then
      -- Check for primitives first, then any named types we've already
      -- processed
      if PRIMITIVES[decoded] then
         return PRIMITIVES[decoded]
      elseif link_table[decoded] then
         return link_table[decoded]
      else
         error([[Cannot find linked schema "]]..decoded..[["]])
      end

   elseif type(decoded) == "number" then
      error([[JSON number doesn't represent a valid schema]])

   elseif type(decoded) ~= "table" then
      error([[Unknown decoded JSON object]])
   end

   -- We've got a table.  Is it a JSON array or object?
   local json_type = getmetatable(decoded).__jsontype
   if json_type == "array" then
      -- Union type
      local schema = UnionSchema:new()
      for _, branch_json in ipairs(decoded) do
         local branch_schema = parse_decoded_json(branch_json, link_table)
         schema:add_branch(branch_schema)
      end
      return schema

   elseif decoded.type == "array" then
      -- Array type
      local items_json = assert(decoded.items, "No items schema for array")
      local items_schema = parse_decoded_json(items_json, link_table)
      return ArraySchema:new(items_schema)

   elseif decoded.type == "enum" then
      local name = assert(decoded.name, "No name for enum")
      local old_schema = link_table[name]

      local schema = EnumSchema:new(name)
      link_table[name] = schema

      local symbols = assert(decoded.symbols, "No symbols for enum")
      for _, sym in ipairs(symbols) do
         if type(sym) ~= "string" then
            error("Invalid enum symbol "..tostring(sym))
         end
         schema:add_symbol(sym)
      end

      if old_schema then
         if schema == old_schema then
            link_table[name] = old_schema
            return old_schema
         else
            error([[Already have a schema named "]]..name..[["]])
         end
      end
      return schema

   elseif decoded.type == "fixed" then
      local name = assert(decoded.name, "No name for fixed")
      local old_schema = link_table[name]

      local size = assert(decoded.size, "No size for fixed")
      if type(size) ~= "number" then
         error("Fixed size must be a number")
      end
      local schema = FixedSchema:new(name, size)

      if old_schema then
         if schema == old_schema then
            link_table[name] = old_schema
            return old_schema
         else
            error([[Already have a schema named "]]..name..[["]])
         end
      end
      link_table[name] = schema
      return schema

   elseif decoded.type == "map" then
      -- Map type
      local values_json = assert(decoded.values, "No values schema for map")
      local values_schema = parse_decoded_json(values_json, link_table)
      return MapSchema:new(values_schema)

   elseif decoded.type == "record" then
      local name = assert(decoded.name, "No name for record")
      local old_schema = link_table[name]

      local schema = RecordSchema:new(name)
      link_table[name] = schema

      local fields = assert(decoded.fields, "No fields for record")
      for _, field in ipairs(fields) do
         if type(field) ~= "table" then
            error("Invalid record field "..tostring(field))
         end

         local field_name = assert(field.name, "No name for record field")
         local field_type = assert(field.type, "No type for record field")
         local field_schema = parse_decoded_json(field_type, link_table)
         schema:add_field(field_name, field_schema)
      end

      if old_schema then
         if schema == old_schema then
            link_table[name] = old_schema
            return old_schema
         else
            error([[Already have a schema named "]]..name..[["]])
         end
      end
      return schema

   elseif type(decoded.type) == "string" then
      return parse_decoded_json(decoded.type, link_table)

   else
      error("Invalid JSON schema")
   end
end

function Schema:new(json_str)
   local decoded, _, err = json.decode(json_str)
   if decoded then
      return parse_decoded_json(decoded, {})
   else
      error(err)
   end
end


------------------------------------------------------------------------
-- Helper constructors for compound types

-- The constructors for enum, fixed, and record schemas all take in a
-- name, followed by a Lua table describing the contents of the type.
-- We do this by only taking in the name in this function, which returns
-- a function that takes in the Lua table.  This gives us an overall
-- syntax that doesn't require parentheses:
--
--   local schema = enum "color" { "RED", "GREEN", "BLUE" }

-- Links
--
--   local schema = link "schema_name"
--
-- For links, we maintain a hash table of each named schema that we've
-- constructed so far.  Normally, you'd think that the innermost part of
-- a schema constructor expression would be evaluated first, meaning
-- that the link wouldn't be able to immediately see the schema that it
-- points to.  Luckily, however, the curried-function approach that we
-- use to construct records means that we can create the (empty) record
-- schema, and assign it into the hash table, *before* the second
-- function (which defines the record's fields) is evaluated.  Nice!

local LINK_TARGETS = {}
local LINK_DEPTH = 0

local function init_links()
   LINK_DEPTH = LINK_DEPTH + 1
end

local function done_links()
   LINK_DEPTH = LINK_DEPTH - 1
   if LINK_DEPTH == 0 then
      LINK_TARGETS = {}
   end
end

local function save_link(name, schema)
   LINK_TARGETS[name] = schema
end

function link(name)
   if not LINK_TARGETS[name] then
      error("No schema named "..name)
   else
      --print("--- link "..name)
      return LINK_TARGETS[name]
   end
end

-- Arrays and maps
--
--   local schema = array { item_schema }
--   local schema = array(item_schema)

function array(args)
   --print("--- array")
   init_links()
   local item_schema = args[1] or args
   done_links()
   return ArraySchema:new(item_schema)
end

function map(args)
   --print("--- map")
   init_links()
   local value_schema = args[1] or args
   done_links()
   return MapSchema:new(value_schema)
end

-- Enums
--
--   local schema = enum "color" { "RED", "GREEN", "BLUE" }

function enum(name)
   --print("--- enum "..name)
   init_links()
   return function (symbols)
      local schema = EnumSchema:new(name)
      for _, symbol_name in ipairs(symbols) do
         schema:add_symbol(symbol_name)
      end
      save_link(name, schema)
      done_links()
      return schema
   end
end

-- Fixeds
--
--   local schema = fixed "ipv4" { size=4 }
--   local schema = fixed "ipv4"(4)

function fixed(name)
   --print("--- fixed "..name)
   init_links()
   return function (args)
      local size
      if type(args) == "table" then
         size = args.size
      else
         size = tonumber(args)
      end
      local schema = FixedSchema:new(name, size)
      save_link(name, schema)
      done_links()
      return schema
   end
end

-- Records
--
--   local schema = record "packet" {
--      timestamp = record "timestamp" {
--         value = "long",
--      },
--      full_length = "long",
--      packet = "bytes",
--   }
--
-- OR
--
--   local schema = record "packet" {
--      {timestamp = record "timestamp" {
--         value = "long",
--      }},
--      {full_length = "long"},
--      {packet = "bytes"},
--   }
--
-- In the first syntax, the entries in the Lua table are keyed by name,
-- and are therefore unordered, so you don't know in advance which order
-- the fields of the record will be in.  Most of the time, that's not
-- good.
--
-- In the second syntax, the outer table is an array-like table, with
-- numerical indices, and each value is a single-element table.  This
-- lets us ensure that the fields appear in the schema in the same order
-- they appear in the Lua source code.

function record(name)
   --print("--- record "..name)
   init_links()
   local schema = RecordSchema:new(name)
   save_link(name, schema)
   return function (fields)
      for _, field_table in ipairs(fields) do
         local field_name, field_schema = next(field_table)
         schema:add_field(field_name, field_schema)
      end
      for field_name, field_schema in pairs(fields) do
         if type(field_name) == "string" then
            schema:add_field(field_name, field_schema)
         end
      end

      done_links()
      return schema
   end
end


-- Unions
--
--   local schema = union { branch_schemas }

function union(branches)
   --print("--- union")
   init_links()
   local schema = UnionSchema:new()
   for _, branch_schema in ipairs(branches) do
      schema:add_branch(branch_schema)
   end
   done_links()
   return schema
end
