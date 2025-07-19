# chep_extention/Makefile

MODULES = chep_extention
EXTENSION = chep_extention
DATA = chep_extention--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)