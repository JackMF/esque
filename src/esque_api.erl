
-module(esque_api).

-export[start/0, stop/1].


start() ->
	Dispatch = cowboy_router:compile([
        {'_', [
            {"/", cowboy_static, {priv_file, esque, "index.html"}},
            {"/websocket", esque_consumer, []},
            {"/static/[...]", cowboy_static, {priv_dir, esque, "static"}}
		]}
	]),
	{ok, _} = cowboy:start_clear(http, [{port, 8080}], #{
		env => #{dispatch => Dispatch}
	}).
	% websocket_sup:start_link().

stop(_State) ->
	ok = cowboy:stop_listener(http).