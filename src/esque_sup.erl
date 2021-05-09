-module(esque_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {}).

init({}) ->
    esque_reg:init(), %Starting consumer registry
    {ok,
        {
            % Restart strategy
            #{
                strategy  => one_for_one, % optional
                intensity => 1,           % optional
                period    => 10           % optional
            },
            % Children
            [
                #{
                id       => consumer_sup,
                start    => {esque_consumer_sup, start_link, []},
                restart  => permanent,
                shutdown => 5000,
                type     => supervisor,
                modules  => [exorom_batch_producer_sup]
            }

            ]
        }
    }.
