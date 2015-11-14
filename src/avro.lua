-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011-2015, RedJack, LLC.
-- All rights reserved.
--
-- Please see the COPYING file in this distribution for license details.
------------------------------------------------------------------------

local AC = require "avro.c"
local ACC = require "avro.constants"
local AS = require "avro.schema"
local AW = require "avro.wrapper"

local pairs = pairs
local print = print
local setmetatable = setmetatable
local string = string

module "avro"

------------------------------------------------------------------------
-- Constants

for k,v in pairs(ACC) do
   if string.sub(k,1,1) ~= "_" then
      _M[k] = v
   end
end


------------------------------------------------------------------------
-- Copy a bunch of public functions from the submodules.

ArraySchema = AS.ArraySchema
EnumSchema = AS.EnumSchema
FixedSchema = AS.FixedSchema
LinkSchema = AS.LinkSchema
MapSchema = AS.MapSchema
RecordSchema = AS.RecordSchema
Schema = AS.Schema
UnionSchema = AS.UnionSchema

ResolvedReader = AC.ResolvedReader
ResolvedWriter = AC.ResolvedWriter
open = AC.open
raw_decode_value = AC.raw_decode_value
raw_encode_value = AC.raw_encode_value
raw_value = AC.raw_value
wrapped_value = AC.wrapped_value

get_wrapper_class = AW.get_wrapper_class
set_wrapper_class = AW.set_wrapper_class
Wrapper = AW.Wrapper
ArrayValue = AW.ArrayValue
CompoundValue = AW.CompoundValue
LongValue = AW.LongValue
MapValue = AW.MapValue
RecordValue = AW.RecordValue
ScalarValue = AW.ScalarValue
StringValue = AW.StringValue
UnionValue = AW.UnionValue

boolean = AS.boolean
bytes = AS.bytes
double = AS.double
float = AS.float
int = AS.int
long = AS.long
null = AS.null
_M.string = AS.string  -- need the _M b/c we import Lua's string above

array = AS.array
enum = AS.enum
fixed = AS.fixed
link = AS.link
map = AS.map
record = AS.record
union = AS.union
