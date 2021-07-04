-module(esque_reg).
-export([
    init/0,
    shutdown/0,
    reg_consumer/4,
    unreg_consumer/3,
    update_offset/4,
    get_consumers_pids/2
    ]).


-define(TABLE_NAME, reg).

-type topic() :: atom().
-type partition() :: integer().
-type group() :: atom().
-type offset() :: integer().
%-type row() :: {{topic(), partition(), group()}, pid(), offset()}.
-define(OFFSET_POS, 3). %the offset position in the row 

init() ->
    Opts = [set, public, named_table, {read_concurrency, true}, {write_concurrency, false}],
    ?TABLE_NAME = ets:new(?TABLE_NAME, Opts).

shutdown() ->
    true = ets:delete(?TABLE_NAME).


%%Note will probably have to check before reg that a consumer doesnt already exist.
-spec reg_consumer(Topic :: topic(), Partition :: partition(), Group :: group(), Pid:: pid()) -> true.
reg_consumer(Topic, Partition, Group, Pid) ->
    true = ets:insert(?TABLE_NAME, {{Topic, Partition, Group}, Pid, 0}).

-spec unreg_consumer(Topic :: topic(), Partition :: partition(), Group :: group()) -> true.
unreg_consumer(Topic, Partition, Group) ->
    true = ets:delete(?TABLE_NAME, {{Topic, Partition, Group}}).

-spec update_offset(Topic :: topic(), Partition :: partition(), Group :: group(), Offset :: offset()) -> true.
update_offset(Topic, Partition, Group, Offset) ->
    ets:update_element(?TABLE_NAME,{Topic, Partition, Group}, {?OFFSET_POS, Offset}).

-spec get_consumers_pids(Topic :: topic(), Partition :: partition()) -> list(pid()).
get_consumers_pids(Topic, Partition) ->
    lists:flatten(ets:match(?TABLE_NAME, {{Topic, Partition, '_'}, '$1', '_'})).
