-module(esque_listener).
-behaviour(gen_statem).
-behaviour(ranch_protocol).
% API.
-export([start_link/3]).



-export([
    callback_mode/0,
    init/1,
    connected/3,
    terminate/3,
    code_change/4
]).


-define(TIMEOUT, 60000).

-record(state, {socket, transport}).

%% API.

start_link(Ref, Transport, Opts) ->
	{ok, proc_lib:spawn_link(?MODULE, init, [{Ref, Transport, Opts}])}.

%% gen_statem.
callback_mode() ->
	state_functions.

init({Ref, Transport, _Opts = []}) ->
	{ok, Socket} = ranch:handshake(Ref),
	ok = Transport:setopts(Socket, [{active, once}, {packet, line}]),
	gen_statem:enter_loop(?MODULE, [], connected,
		#state{socket=Socket, transport=Transport},
		[?TIMEOUT]).

connected(info, {tcp, Socket, Data}, _StateData=#state{
		socket=Socket, transport=Transport})
		when byte_size(Data) > 1 ->
		
	Transport:setopts(Socket, [{active, once}]),
	ToReturn = handle_socket_data(line_deliminate(Data)),
	Transport:send(Socket, ToReturn),
	{keep_state_and_data, ?TIMEOUT};
connected(info, {tcp_closed, _Socket}, _StateData) ->
	{stop, normal};
connected(info, {tcp_error, _, Reason}, _StateData) ->
	{stop, Reason};
connected({call, From}, _Request, _StateData) ->
	gen_statem:reply(From, ok),
	keep_state_and_data;
connected(cast, _Msg, _StateData) ->
	keep_state_and_data;
connected(timeout, _Msg, _StateData) ->
	{stop, normal};
connected(_EventType, _Msg, _StateData) ->
	{stop, normal}.

terminate(Reason, StateName, StateData=#state{
		socket=Socket, transport=Transport})
		when Socket=/=undefined andalso Transport=/=undefined ->
	catch Transport:close(Socket),
	terminate(Reason, StateName,
		StateData#state{socket=undefined, transport=undefined});
terminate(_Reason, _StateName, _StateData) ->
	ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
	{ok, StateName, StateData}.

%% Internal.
line_deliminate(Data) ->
	hd(string:split(Data, "\r\n")).
handle_socket_data(<<"1", Partitions:1/binary, TopicName/binary>>) ->
	%1 means create a new topic
	io:format("The topic name is ~p ~n Partitions is ~p  ~n", [TopicName,Partitions]),
	TopicName.

% reverse_binary(B) when is_binary(B) ->
% 	[list_to_binary(lists:reverse(binary_to_list(
% 		binary:part(B, {0, byte_size(B)-2})
% 	))), "\r\n"].