-module(esque_consumer).
-behaviour(gen_statem).

%API
-export([start_link/3,
		send/3]).
%% Callbacks
-export([
	callback_mode/0,
	init/1,
	catch_up/3,
	listen/3
]).

-record(state, {
   group :: binary(),
   topic :: atom(),
   partition :: non_neg_integer(),
   next_offset_to_send_from :: non_neg_integer()
}).
-define(SERVER(Topic, Partition), {via, gproc, {n, l, {Topic, Partition}}}).

	
%%Api
send(Topic, Partition, Msgs) ->
	gen_statem:cast(
		{via, gproc, {n, l, {Topic, Partition}}},
		{send, Msgs}). 


callback_mode() ->
    [state_functions, state_enter].

start_link(Group, Topic, Partition) ->
	gen_statem:start_link(?SERVER(Topic, Partition), ?MODULE, [Group, Topic, Partition], []).

init([Group, Topic, Partition]) ->
    process_flag(trap_exit, true),
    {ok, catch_up, #state{group=Group, 
    					topic=Topic, 
    					partition=Partition, 
    					next_offset_to_send_from=0}}.

catch_up(enter, _OldState, _State) ->
	{keep_state_and_data, {state_timeout, 0, send_batch}};
catch_up(state_timeout, send_batch, #state{group=_Group, 
										topic=Topic, 
										partition=Partition,  
										next_offset_to_send_from=Offset}=State) ->

	 Rows = esque_qs:all_messages_from_offset(Topic, Partition, Offset),
	 send_batch(Rows),
	 {next_state, listen, State}.	
listen(enter, _OldState, _State) ->
	keep_state_and_data;
listen(cast, {send, Msgs} , _State) ->
	send_batch(Msgs),
	keep_state_and_data.
	 
send_batch(Rows) ->
	io:format("Send rows ~p", [Rows]).



