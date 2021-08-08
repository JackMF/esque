-module(esque_qs).

-export([
    init/0,
    shutdown/0,
    new/2,
    delete/2,
    put/4,
    shard/2
    % all_messages_from_offset/3
    ]).

-define(CHANGELOG, "changelog").
-define(OFFSET_TABLE, offsets).

-define(STORE_ETS_OPTS, [set, named_table, public, {read_concurrency, true}, {write_concurrency, false}]).
-define(LOG_DETS_OPTS, []).

-define(DETS_DIR, "dets/").

% -type value() :: binary().
% -type key() :: binary().
% -type last_offset() :: integer().


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



%%%Offset updates
get_last_offset(QName, Partition) ->
    StoreName = shard(QName, Partition),
    [{StoreName, Offset}] = ets:lookup(?OFFSET_TABLE, StoreName),
    Offset.

update_offset(QName, Partition, NewOffset) ->
    StoreName = shard(QName, Partition),
    ets:insert(?OFFSET_TABLE, {StoreName, NewOffset}).



merge_insert(StoreName, Key, NewValue, NewOffset) ->
    case ets:lookup(StoreName, Key) of
        [] ->
            true = ets:insert(StoreName, {Key, NewValue, NewOffset});
        [{Key, Value, _LastOffset}] ->
            true = ets:insert(StoreName, {Key, maps:merge(Value, NewValue), NewOffset})
    end.

% -spec all_messages_from_offset(QName :: atom(), Partition :: integer(), Offset :: offset()) -> list(row()).
% all_messages_from_offset(QName, Partition, Offset) ->
%     TableName = shard(QName, Partition),
%     %%We could avoid a reverse here if we were to start at current offset then work backwards to desired out put and prepend.
%     %That is use ets:prev instead of ets:next
%     lists:reverse(all_messages_from_offset_helper(TableName, Offset, [])).

% -spec all_messages_from_offset_helper(TableName :: atom(), Offset :: integer()|'$end_of_table', Messages :: list()) -> list().
% all_messages_from_offset_helper(_TableName, '$end_of_table', Messages) ->
%     Messages;
% all_messages_from_offset_helper(TableName, Offset, Messages) ->
%     case ets:lookup(TableName, Offset) of
%         [Message] ->
%             all_messages_from_offset_helper(TableName, ets:next(TableName, Offset), [Message|Messages]);
%         [] ->
%             Messages
%     end.
%%%Helpers
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