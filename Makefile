PROJECT = esque
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0

DEPS = ranch gproc

dep_ranch = git https://github.com/ninenines/ranch.git
dep_ranch_commit=master

dep_gproc = git https://github.com/uwiger/gproc.git
dep_gproc_commit=master

TEST_DEPS = meck eunit_formatters
EUNIT_OPTS = cover, no_tty, {report, {eunit_progress, [colored, profile]}}
include erlang.mk

