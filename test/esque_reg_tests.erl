-module(esque_reg_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_GROUP, test_group).
-define(TEST_TOPIC, test_topic).
-define(TEST_PARTITIONS, 1).


test_setup() ->
    esque_qs:new(?TEST_TOPIC, ?TEST_PARTITIONS),
    esque_reg:init().
   

test_teardown(_) ->
    esque_qs:delete(?TEST_TOPIC, ?TEST_PARTITIONS),
	esque_reg:shutdown().

all_test_() ->
    {
        setup,
        fun test_setup/0,
        fun test_teardown/1,
        [
            {"store and lookup pid", fun store_look_up/0}

        ]
    }.

store_look_up() ->
    %When no consumers are regs then there should be no consumers
    ?assertEqual([], esque_reg:get_consumers(?TEST_TOPIC, 0)),
    %start a consumer
    {ok, Pid} = esque_consumer:start_consumer(?TEST_GROUP, ?TEST_TOPIC, 0),

    %now we have started a consumer we should find some conusmers
    ?assertEqual([Pid], esque_reg:get_consumers(?TEST_TOPIC, 0)).





    

