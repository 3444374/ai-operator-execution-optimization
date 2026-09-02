# Test-only module: build the production plan codec in a real PG backend.
# No production SQL entry point, test GUC, or exported extension symbol is added.
MODULE_big = semloom_plan_contract_test
OBJS = plan_contract.o sem_plan_spec.o generation_profile.o
vpath %.c ../../src
PG_CPPFLAGS = -I../../src
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
