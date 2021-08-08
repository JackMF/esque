PROJECT = esque
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0


TEST_DEPS = meck eunit_formatters
EUNIT_OPTS = cover, no_tty, verbose, {report, {eunit_progress, [colored, profile]}}
include erlang.mk

