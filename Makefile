PROJECT = esque
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0


TEST_DEPS = meck eunit_formatters

DEPS = cowboy


dep_cowboy_commit = 2.9.0


EUNIT_OPTS = cover, no_tty, verbose, {report, {eunit_progress, [colored, profile]}}
include erlang.mk

