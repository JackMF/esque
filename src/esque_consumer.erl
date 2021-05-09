-module(esque_consumer).
-behaviour(gen_statem).
-include_lib("consumer_state.hrl").

%

%API
-export([start_consumer/3,
		send/3]).
%% Callbacks
-export([
	callback_mode/0,
	init/1,
	catch_up/3,
	listen/3
]).

	
%%Api
send(Topic, Partition, Msgs) ->
	lists:foreach(
		fun(ConsumerPid) -> 
			gen_statem:cast(ConsumerPid, {send, Msgs}) 
		end, 
		esque_reg:get_consumers(Topic, Partition)
	).

callback_mode() ->
    [state_functions, state_enter].

start_consumer(Group, Topic, Partition) ->
	{ok, Pid} = gen_statem:start_link(?MODULE, [Group, Topic, Partition], []),
	true = esque_reg:reg_consumer(Topic, Partition, Group, Pid),
	{ok, Pid}.

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



