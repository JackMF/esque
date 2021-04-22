-module(esque_consumer_sup).

-behaviour(supervisor).

%% callbacks
-export([
    start_link/0,
    init/1
]).

%% api
-export([
    start_consumer/3
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {}).
init({}) ->
     SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 60,
        period => 5
    },
    Children = [#{
        id => exotic,
        start => {esque_consumer, start_link, []},
        restart => permanent,
        type => worker
    }],
    {ok, {SupFlags, Children}}.

start_consumer(Group, Topic, Partition) ->
    supervisor:start_child(?MODULE, [Group, Topic, Partition]).

