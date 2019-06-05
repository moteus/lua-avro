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

local avro = require "avro.module"

------------------------------------------------------------------------
-- Constants

for k,v in pairs(ACC) do
   if string.sub(k,1,1) ~= "_" then
      avro[k] = v
   end
end


------------------------------------------------------------------------
-- Copy a bunch of public functions from the submodules.

avro.ArraySchema = AS.ArraySchema
avro.EnumSchema = AS.EnumSchema
avro.FixedSchema = AS.FixedSchema
avro.LinkSchema = AS.LinkSchema
avro.MapSchema = AS.MapSchema
avro.RecordSchema = AS.RecordSchema
avro.Schema = AS.Schema
avro.UnionSchema = AS.UnionSchema

avro.ResolvedReader = AC.ResolvedReader
avro.ResolvedWriter = AC.ResolvedWriter
avro.open = AC.open
avro.raw_decode_value = AC.raw_decode_value
avro.raw_encode_value = AC.raw_encode_value
avro.raw_value = AC.raw_value
avro.wrapped_value = AC.wrapped_value

avro.get_wrapper_class = AW.get_wrapper_class
avro.set_wrapper_class = AW.set_wrapper_class
avro.Wrapper = AW.Wrapper
avro.ArrayValue = AW.ArrayValue
avro.CompoundValue = AW.CompoundValue
avro.LongValue = AW.LongValue
avro.MapValue = AW.MapValue
avro.RecordValue = AW.RecordValue
avro.ScalarValue = AW.ScalarValue
avro.StringValue = AW.StringValue
avro.UnionValue = AW.UnionValue

avro.boolean = AS.boolean
avro.bytes = AS.bytes
avro.double = AS.double
avro.float = AS.float
avro.int = AS.int
avro.long = AS.long
avro.null = AS.null
avro.string = AS.string  -- need the _M b/c we import Lua's string above

avro.array = AS.array
avro.enum = AS.enum
avro.fixed = AS.fixed
avro.link = AS.link
avro.map = AS.map
avro.record = AS.record
avro.union = AS.union

return avro
