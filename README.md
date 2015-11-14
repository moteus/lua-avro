# Lua Avro bindings

This package provides Lua bindings for the [Avro][] data serialization
framework.  It relies on Avro's C bindings to do the heavy lifting.

## Installation

To install, use the Makefile:

``` console
$ make PREFIX=/usr/local
$ sudo make install PREFIX=/usr/local
```

We also provide [rockspecs](../rockspecs), so that you can install using
[LuaRocks][].

## Usage

There's unfortunately not much in the way of documentation just yet.  You can
see some example usages in our [test suite](../src/avro/tests).

[Avro]: http://avro.apache.org/
[LuaRocks]: https://luarocks.org/

## License

`lua-avro-c` is released under the terms of the MIT/X11 license.  See the
[COPYING](COPYING) file for more details.
