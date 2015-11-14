package = "lua-avro"
version = "0.1"

source = {
   url = "git://github.com/redjack/lua-avro-c.git",
   tag = "0.1"
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
      avro = {
         sources = {"src/avro.c"},
         libraries = {"avro"},
         incdirs = {"$(AVRO_INCDIR)"},
         libdirs = {"$(AVRO_LIBDIR)"},
      },
   },
}
