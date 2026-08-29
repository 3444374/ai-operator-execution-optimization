EXTENSION = semloom_pg
MODULE_big = semloom_pg
OBJS = \
	src/extension.o \
	src/marker.o \
	src/provider.o \
	src/pg_semantic_runtime.o \
	src/recording_provider.o \
	src/sem_filter_machine.o \
	src/sem_filter_path.o \
	src/sem_map_machine.o \
	src/sem_operator_machine.o \
	src/sem_path_common.o \
	src/sem_path.o \
	src/sem_pump.o \
	src/sem_scan.o \
	src/uds_provider.o \
	src/wire_v2.o

DATA = sql/semloom_pg--0.1.0.sql
REGRESS = semloom_pg
TAP_TESTS = 1
PGFILEDESC = "semloom_pg - planner-visible semantic operator capability spike"

PG_CPPFLAGS = -Isrc

PG_CONFIG ?= pg_config
SEMLOOM_PG_TARGET_VERSION ?= 18.3
SEMLOOM_PG_ACTUAL_VERSION := $(word 2,$(shell $(PG_CONFIG) --version 2>/dev/null))

ifneq ($(SEMLOOM_PG_ACTUAL_VERSION),$(SEMLOOM_PG_TARGET_VERSION))
$(error semloom_pg targets PostgreSQL $(SEMLOOM_PG_TARGET_VERSION); PG_CONFIG reports $(SEMLOOM_PG_ACTUAL_VERSION))
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
