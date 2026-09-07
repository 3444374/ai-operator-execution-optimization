EXTENSION = semloom_pg
MODULE_big = semloom_pg
OBJS = \
	src/extension.o \
	src/planner/marker_identity.o \
	src/semantics/generation_profile.o \
	src/marker.o \
	src/provider/provider.o \
	src/executor/pg_semantic_runtime.o \
	src/provider/recording_provider.o \
	src/planner/sem_filter_calibration.o \
	src/planner/sem_filter_cost.o \
	src/semantics/sem_filter_machine.o \
	src/planner/sem_filter_path.o \
	src/semantics/sem_map_machine.o \
	src/semantics/sem_message_writer.o \
	src/semantics/sem_operator_machine.o \
	src/semantics/sem_text.o \
	src/semantics/semantic_map_contract.o \
	src/planner/sem_plan_spec.o \
	src/planner/sem_path_common.o \
	src/planner/sem_map_path.o \
	src/executor/sem_pump.o \
	src/executor/sem_scan.o \
	src/provider/uds_provider.o \
	src/provider/wire/wire_common.o \
	src/provider/wire/wire_semantic.o \
	src/provider/wire/wire_v2.o \
	src/provider/wire/wire_v3.o \
	src/provider/wire/wire_v4.o

DATA = sql/semloom_pg--0.1.0.sql \
	sql/semloom_pg--0.2.0.sql \
	sql/semloom_pg--0.1.0--0.2.0.sql
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
