# chep_extention/Makefile

MODULE_big = chep_extention
OBJS = \
	$(WIN32RES) \
	chep_extention.o

EXTENSION = chep_extention
DATA = chep_extention--1.0.sql
PGFILEDESC = "chep_extention- my Postgres Scheduler"

LDFLAGS_SL += $(filter -lm, $(LIBS))

NO_INSTALLCHECK = 1

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_scheduler
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
