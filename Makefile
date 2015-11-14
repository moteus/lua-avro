# -*- coding: utf-8 -*-
#----------------------------------------------------------------------
# Copyright © 2011-2015, RedJack, LLC.
# All rights reserved.
#
# Please see the COPYING file in this distribution for license details.
#----------------------------------------------------------------------

all: test-prereqs build

.PHONY: all test-prereqs build build-lua build-c clean install install-lua install-c

# Project-specific variables

TEST_MODULE = avro/test.lua
VERSION = $(shell ./version.sh)

# How verbose shall we be?

V=

# Installation location variables.  These will be overridden by luarocks
# if you install that way.

USE_LUAROCKS=1
DESTDIR=
PREFIX=/usr/local
LUA_INCDIR=$(PREFIX)/include
LUA_LIBDIR=$(PREFIX)/lib/lua/5.1
LUA_SHAREDIR=$(PREFIX)/share/lua/5.1

# Other configuration variables.  These will also be set by luarocks.

ifeq ($(shell uname -s),Darwin)
LIBFLAG=-bundle -undefined dynamic_lookup -all_load
else
LIBFLAG=-shared
endif
BUILD_DIR=build
CFLAGS= -g -O2 -Wall -Werror -fPIC
LDFLAGS=

ifeq (,$(USE_LUAROCKS))
LUAROCKS_LOADER=
else
LUAROCKS_LOADER=-lluarocks.loader
endif

# Prerequisites.

test-prereqs:
	@echo Checking for Avro C library...
	@pkg-config 'avro-c >= 1.5.0' --exists --print-errors

AVRO_CFLAGS := $(shell pkg-config avro-c --cflags)
AVRO_LDFLAGS := $(shell pkg-config avro-c --libs)

# Build rules

ifeq (,$(V))
QUIET_CC   = @echo '   CC  ' $@;
QUIET_CP   = @echo '   CP  ' $@;
QUIET_LINK = @echo '   LINK' $@;
else
QUIET_CC   =
QUIET_CP   =
QUIET_LINK =
endif

build: build-lua build-c

SRC_LUA := $(shell find src -type f -name '*.lua')
BUILD_LUA := $(patsubst src/%,build/%,$(SRC_LUA))
INSTALLED_LUA := $(patsubst src/%,$(DESTDIR)$(LUA_SHAREDIR)/%,$(SRC_LUA))

build-lua: $(BUILD_LUA)

build/%.lua: src/%.lua
	@mkdir -p $(dir $@)
	$(QUIET_CP)cp $< $@

SRC_C := $(shell find src -type f -name '*.c')
BUILD_O_C := $(patsubst %.c,%.o,$(patsubst src/%,build/%,$(SRC_C)))
BUILD_SO_C := $(patsubst %.o,%.so,$(BUILD_O_C))
INSTALLED_C := $(patsubst build/%,$(DESTDIR)$(LUA_LIBDIR)/%,$(BUILD_SO_C))

build-c: $(BUILD_O_C) $(BUILD_SO_C)

build/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(QUIET_CC)$(CC) -I$(LUA_INCDIR) -o $@ $(CFLAGS) -c $(AVRO_CFLAGS) $<

build/%.so: build/%.o
	@mkdir -p $(dir $@)
	$(QUIET_LINK)$(CC) -o $@ $(LIBFLAG) $(AVRO_LDFLAGS) $<

test: build
	@echo Testing in Lua...
	@cd $(BUILD_DIR) && lua $(LUAROCKS_LOADER) $(TEST_MODULE)
	@echo Testing in LuaJIT...
	@cd $(BUILD_DIR) && luajit $(LUAROCKS_LOADER) $(TEST_MODULE)

clean:
	@echo Cleaning...
	@rm -rf build

install: install-lua install-c
	@echo Installing...

install-lua: $(INSTALLED_LUA)

$(DESTDIR)$(LUA_SHAREDIR)/%.lua: build/%.lua
	@install -d -m 0755 $(dir $@)
	$(QUIET_INSTALL)install -m 0644 $< $@

install-c: $(INSTALLED_C)

$(DESTDIR)$(LUA_LIBDIR)/%.so: build/%.so
	@install -d -m 0755 $(dir $@)
	$(QUIET_INSTALL)install -m 0755 $< $@
