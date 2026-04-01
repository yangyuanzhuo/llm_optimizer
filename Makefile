EXTENSION = llm_optimizer
MODULE_big = llm_optimizer
OBJS = src/llm_optimizer.o
DATA = sql/llm_optimizer--1.0.sql

SHLIB_LINK += -lcurl

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
