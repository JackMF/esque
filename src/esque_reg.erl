-module(esque_reg).
-export([
    init/0,
    shutdown/0,
	reg_consumer/4,
    unreg_consumer/3,
    get_consumers/2
	]).


-define(TABLE_NAME, reg).

-type topic() :: atom().
-type partition() :: integer().
-type group() :: atom().
% -type row() :: {{topic(), partition(), group()}, pid()}.


init() ->
    Opts = [set, public, named_table, {read_concurrency, true}, {write_concurrency, false}],
    ?TABLE_NAME = ets:new(?TABLE_NAME, Opts).

shutdown() ->
    true = ets:delete(?TABLE_NAME).

-spec reg_consumer(Topic :: topic(), Partition :: partition(), Group :: group(), Pid:: pid()) -> true.
reg_consumer(Topic, Partition, Group, Pid) ->
    true = ets:insert(?TABLE_NAME, {{Topic, Partition, Group}, Pid}).

-spec unreg_consumer(Topic :: topic(), Partition :: partition(), Group :: group()) -> true.
unreg_consumer(Topic, Partition, Group) ->
    true = ets:delete(?TABLE_NAME, {{Topic, Partition, Group}}).


-spec get_consumers(Topic :: topic(), Partition :: partition()) -> list(pid()).
get_consumers(Topic, Partition) ->
    lists:flatten(ets:match(?TABLE_NAME, {{Topic, Partition, '_'}, '$1'})).