EXTENSION = semloom_pg
MODULE_big = semloom_pg
OBJS = \
	src/extension.o \
	src/marker.o \
	src/sem_path.o \
	src/sem_scan.o

DATA = sql/semloom_pg--0.1.0.sql
REGRESS = semloom_pg
PGFILEDESC = "semloom_pg - planner-visible SemMap capability spike"

PG_CPPFLAGS = -Isrc

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
