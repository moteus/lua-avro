-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011-2015, RedJack, LLC.
-- All rights reserved.
--
-- Please see the COPYING file in this distribution for license details.
------------------------------------------------------------------------

-- Loads either avro.legacy.avro or avro.ffi.avro, depending on whether
-- the LuaJIT FFI module is available.

local ffi_present = pcall(require, "ffi")
local mod
if ffi_present then
   --print("Loading ffi version")
   mod = require("avro.ffi.avro")
else
   --print("Loading legacy version")
   mod = require("avro.legacy.avro")
end
mod.ffi_present = ffi_present
avro.c = mod
return mod
