-module(esque_consumer_tests).
-include_lib("eunit/include/eunit.hrl").


-include_lib("consumer_state.hrl").


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
            {"start_consumer", fun start_consumer/0}

        ]
    }.

start_consumer() ->

    {ok, Pid} = esque_consumer:start_consumer(?TEST_GROUP, ?TEST_TOPIC, 0),
    {InState, #state{topic=Topic, partition=Partition, group=Group}} = sys:get_state(Pid),
    ?assertEqual(listen, InState),
    ?assertEqual(?TEST_TOPIC, Topic),
    ?assertEqual(?TEST_GROUP, Group),
    ?assertEqual(0, Partition).







    

