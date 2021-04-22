-module(esque_app).

-behaviour(application).

-export([
    start/2,
    stop/1
]).

start(_StartType, _StartArgs) ->
	
	% {ok, _} = ranch:start_listener(tcp_echo,
	% ranch_tcp, #{socket_opts => [{port, 5555}]},
    % esque_listener, []),
    {ok, _} = esque_sup:start_link().

stop(_State) ->
    ok.
