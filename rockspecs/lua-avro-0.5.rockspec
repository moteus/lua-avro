package = "lua-avro"
version = "0.5"

source = {
   url = "git://github.com/redjack/lua-avro-c.git",
   tag = "0.5"
}

description = {
   summary = "Lua bindings for Avro C library",
   homepage = "https://github.com/redjack/lua-avro",
   license = "MIT/X11"
}

dependencies = {
   "lua >= 5.1",
}

external_dependencies = {
   AVRO = {
      header = "avro.h",
   },
}

build = {
   type = "builtin",
   modules = {
      avro = "src/avro.lua",
      ["avro.constants"] = "src/avro/constants.lua",
      ["avro.wrapper"] = "src/avro/wrapper.lua",
      ["avro.c"] = "src/avro/c.lua",
      ["avro.c.legacy"] = {
         sources = {"src/avro/c/legacy.c"},
         libraries = {"avro"},
         incdirs = {"$(AVRO_INCDIR)"},
         libdirs = {"$(AVRO_LIBDIR)"},
      },
      ["avro.c.ffi"] = "src/avro/c/ffi.lua",
      ["avro.test"] = "src/avro/test.lua",
      ["avro.tests.raw"] = "src/avro/tests/raw.lua",
      ["avro.tests.wrapper"] = "src/avro/tests/wrapper.lua",
   },
}
