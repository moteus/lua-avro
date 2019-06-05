-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011-2015, RedJack, LLC.
-- All rights reserved.
--
-- Please see the COPYING file in this distribution for license details.
------------------------------------------------------------------------

-- Loads either avro.legacy.avro or avro.ffi.avro, depending on whether
-- the LuaJIT FFI module is available.

local avro = require "avro.module"

local ffi_present = pcall(require, "ffi")
if ffi_present then
   -- print("Loading ffi version")
   avro.c = require("avro.ffi.avro")
else
   -- print("Loading legacy version")
   avro.c = require("avro.legacy.avro")
end
avro.c.ffi_present = ffi_present

return avro.c
