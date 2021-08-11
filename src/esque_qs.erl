-module(esque_qs).


%%Setup
-export([
    init/0,
    shutdown/0
]).
%%Making logs
-export([
    new/2,
    delete/2
]).
%%Putting items
-export([put/4]).
%%Getting items.
-export(
    [lookup_log_by_offset/3,
    lookup_store_by_key/3,
    all_messages_from_offset/3
    ]).

-export([shard/2]).

-define(CHANGELOG, "log").
-define(OFFSET_TABLE, offsets).

-define(STORE_ETS_OPTS, [set, named_table, public, {read_concurrency, true}, {write_concurrency, false}]).
-define(LOG_DETS_OPTS, []).

-define(DETS_DIR, "dets/").

% -type value() :: binary().
% -type key() :: binary().
% -type last_offset() :: integer().




%%%%%%%% Set up %%%%%%
-spec init() -> ok.
init() ->
    ?OFFSET_TABLE = ets:new(?OFFSET_TABLE, [set, named_table, public, {read_concurrency, true}, {write_concurrency, false}]),
    filelib:ensure_dir(?DETS_DIR),
    ok.
shutdown() ->
    ets:delete(?OFFSET_TABLE),
    file:del_dir_r(?DETS_DIR).


%Makes a new ets table for QName with Partitions number of partitions.	
-spec new(QName :: atom(), Partitions :: integer()) -> ok.
new(QName, Partitions) ->
    lists:foreach(
        fun(Partition) ->
            StoreName = shard(QName, Partition),
            LogName = shard(change_log(QName), Partition),
            StoreName = ets:new(StoreName, ?STORE_ETS_OPTS),
            {ok, LogName} = dets:open_file(LogName, [{file, dets_file_name(LogName)}]),
            true = ets:insert(?OFFSET_TABLE, {StoreName, -1})
        end, lists:seq(0, Partitions-1)).

-spec delete(QName :: atom(), Partitions :: integer()) -> ok.
delete(QName, Partitions) ->
    lists:foreach(
        fun(Partition) ->
            StoreName = shard(QName, Partition),
            true = ets:delete(StoreName),
            LogName = shard(change_log(QName), Partition),
            ok = dets:delete_all_objects(LogName),
            true = ets:delete(?OFFSET_TABLE, StoreName)
        end, lists:seq(0, Partitions-1)).

-spec put(QName :: atom(), Partition :: integer(), 
         Key :: binary(), Value :: binary()) -> ok.

%% Putting items into log and store
put(QName, Partition, Key, Value) ->
    CurrentOffSet = get_last_offset(QName, Partition), 
    put_log(QName, Partition, Key, Value, CurrentOffSet),
    put_store(QName, Partition, Key, Value, CurrentOffSet),
    update_offset(QName, Partition, CurrentOffSet+1),
    esque_consumer:send(QName, Partition, [{CurrentOffSet + 1, Key, Value}]).


put_log(QName, Partition, Key, Value, CurrentOffSet) ->
    LogName = shard(change_log(QName), Partition), 
    ok = dets:insert(LogName, {CurrentOffSet + 1, Key, Value}).

put_store(QName, Partition, Key, Value, CurrentOffSet) ->
    StoreName = shard(QName, Partition),
    merge_insert(StoreName, Key, Value, CurrentOffSet + 1).  

%%% Looking up items
lookup_store_by_key(QName, Partition, Key) ->
    StoreName = shard(QName, Partition),
    case ets:lookup(StoreName, Key) of
        [] ->
            undefined;
        [Row] ->
            Row
    end.

lookup_log_by_offset(QName, Partition, OffSet) ->
    LogName = shard(change_log(QName), Partition), 
    case dets:lookup(LogName, OffSet) of
        [] ->
            undefined;
        [Row] ->
            Row
    end.


all_messages_from_offset(QName, Partition, OffSet) ->
    LogName = shard(change_log(QName), Partition), 
    lists:reverse(all_messages_from_offset_helper(LogName, OffSet, [])).

all_messages_from_offset_helper(_LogName, '$end_of_table', Messages) ->
    Messages;
all_messages_from_offset_helper(LogName, OffSet, Messages) ->
    case dets:lookup(LogName, OffSet) of
        [] ->
            Messages;
        [Message] ->
            all_messages_from_offset_helper(LogName, dets:next(LogName, OffSet), [Message|Messages])
    end.
    
%%%Offsets
get_last_offset(QName, Partition) ->
    StoreName = shard(QName, Partition),
    [{StoreName, Offset}] = ets:lookup(?OFFSET_TABLE, StoreName),
    Offset.
update_offset(QName, Partition, NewOffset) ->
    StoreName = shard(QName, Partition),
    ets:insert(?OFFSET_TABLE, {StoreName, NewOffset}).

%%Helpers    
merge_insert(StoreName, Key, NewValue, NewOffset) ->
    case ets:lookup(StoreName, Key) of
        [] ->
            true = ets:insert(StoreName, {Key, NewValue, NewOffset});
        [{Key, Value, _LastOffset}] ->
            true = ets:insert(StoreName, {Key, esque_merge:deep_merge(Value, NewValue), NewOffset})
    end.

-spec shard(Name :: atom(), Partition :: integer()) -> atom().
shard(Name, Partition) ->
    list_to_atom(
      atom_to_list(Name) ++ "_" ++ integer_to_list(Partition)).

-spec change_log(QName :: atom()) -> atom().
change_log(QName) ->
    list_to_atom(
        atom_to_list(QName) ++ "_" ++ ?CHANGELOG).


dets_file_name(LogName) ->
    "dets/" ++ atom_to_list(LogName).