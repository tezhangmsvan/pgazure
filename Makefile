EXTENSION = pgazure
EXTVERSION = 1.0
MODULE_big = $(EXTENSION)
DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--1.0.sql
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c)) $(patsubst %.cpp,%.o,$(wildcard src/*.cpp))
PG_CPPFLAGS = -Iinclude
PG_CXXFLAGS = -Iinclude -std=c++11
PG_CFLAGS = -Iinclude -std=c99 -Wno-declaration-after-statement
SHLIB_LINK = $(libpq) -lstdc++ -lazurestorage -lcpprest -lboost_system
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
