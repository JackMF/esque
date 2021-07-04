-module(esque_qs).

-export([
	new/2,
	delete/2,
	put/4,
	shard/2,
	all_messages_from_offset/3
	]).


-type value() :: binary().
-type key() :: binary().
-type offset() :: integer().
-type row() :: {offset(), key(), value()}.
%Makes a new ets table for QName with Partitions number of partitions.	
-spec new(QName :: atom(), Partitions :: integer()) -> ok.
new(QName, Partitions) ->
	Opts = [ordered_set, named_table, {read_concurrency, true}, {write_concurrency, false}],
	lists:foreach(
		fun(Partition) ->
			TableName = shard(QName, Partition),
			TableName = ets:new(TableName, Opts)
		end, lists:seq(0, Partitions-1)).

-spec delete(QName :: atom(), Partitions :: integer()) -> ok.
delete(QName, Partitions) ->
	lists:foreach(
		fun(Partition) ->
			TableName = shard(QName, Partition),
			true= ets:delete(TableName)
		end, lists:seq(0, Partitions-1)).

-spec put(QName :: atom(), Partition :: integer(), 
		 Key :: binary(), Value :: binary()) -> ok.
put(QName, Partition, Key, Value) ->
	TableName = shard(QName, Partition),
	CurrentOffSet = get_last_offset(TableName), 
	true = ets:insert(TableName, {CurrentOffSet + 1, Key, Value}),
	esque_consumer:send(QName, Partition, [{CurrentOffSet + 1, Key, Value}]).


-spec get_last_offset(TableName :: atom()) -> offset().
get_last_offset(TableName) ->
	case ets:last(TableName) of
		'$end_of_table' ->
			-1;
		OffSet ->
			OffSet
	end.

-spec all_messages_from_offset(QName :: atom(), Partition :: integer(), Offset :: offset()) -> list(row()).
all_messages_from_offset(QName, Partition, Offset) ->
	TableName = shard(QName, Partition),
	%%We could avoid a reverse here if we were to start at current offset then work backwards to desired out put and prepend.
	%That is use ets:prev instead of ets:next
	lists:reverse(all_messages_from_offset_helper(TableName, Offset, [])).

-spec all_messages_from_offset_helper(TableName :: atom(), Offset :: integer()|'$end_of_table', Messages :: list()) -> list().
all_messages_from_offset_helper(_TableName, '$end_of_table', Messages) ->
	Messages;
all_messages_from_offset_helper(TableName, Offset, Messages) ->
	case ets:lookup(TableName, Offset) of
		[Message] ->
			all_messages_from_offset_helper(TableName, ets:next(TableName, Offset), [Message|Messages]);
		[] ->
			Messages
	end.
%%%Helpers
-spec shard(Name :: atom(), Partition :: integer()) -> atom().
shard(Name, Partition) ->
    list_to_atom(
      atom_to_list(Name) ++ "_" ++ integer_to_list(Partition)).

