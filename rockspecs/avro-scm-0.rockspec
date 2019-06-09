package = "avro"
version = "scm-0"

source = {
  url = "https://github.com/moteus/lua-avro/archive/master.zip",
  dir = "lua-avro-master",
}

description = {
   summary = "Lua bindings for Avro C library",
   homepage = "https://github.com/moteus/lua-avro",
   license = "MIT/X11"
}

dependencies = {
   "lua >= 5.1",
}

external_dependencies = {
   AVRO = {
      header = "avro.h",
      library = "avro",
   },
}

build = {
   type = "builtin",
   modules = {
      avro = "src/avro.lua",
      ["avro.constants"] = "src/avro/constants.lua",
      ["avro.dkjson"] = "src/avro/dkjson.lua",
      ["avro.schema"] = "src/avro/schema.lua",
      ["avro.wrapper"] = "src/avro/wrapper.lua",
      ["avro.c"] = "src/avro/c.lua",
      ["avro.legacy.avro"] = {
         sources = {"src/avro/legacy/avro.c"},
         libraries = {"avro"},
         incdirs = {"$(AVRO_INCDIR)"},
         libdirs = {"$(AVRO_LIBDIR)"},
      },
      ["avro.ffi.avro"] = "src/avro/ffi/avro.lua",
      ["avro.module"] = "src/avro/module.lua",
      ["avro.test"] = "src/avro/test.lua",
      ["avro.tests.raw"] = "src/avro/tests/raw.lua",
      ["avro.tests.schema"] = "src/avro/tests/schema.lua",
      ["avro.tests.wrapper"] = "src/avro/tests/wrapper.lua",
   },
}
