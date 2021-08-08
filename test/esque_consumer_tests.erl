-module(esque_consumer_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("consumer_state.hrl").


-define(TEST_GROUP, test_group).
-define(TEST_TOPIC, test_topic).
-define(TEST_PARTITIONS, 1).
-define(TEST_PARTITION, 0).


test_setup() ->
    esque_qs:init(),
    esque_qs:new(?TEST_TOPIC, ?TEST_PARTITIONS),
    esque_reg:init().
   

test_teardown(_) ->
    esque_qs:delete(?TEST_TOPIC, ?TEST_PARTITIONS),
    esque_qs:shutdown(),
    esque_reg:shutdown().

all_test_() ->
    {
        setup,
        fun test_setup/0,
        fun test_teardown/1,
        [
            {"start_consumer", fun start_consumer/0},
            {"start a consumer and recieve a msg", fun start_and_receive/0}
        ]
    }.

start_consumer() ->
    {ok, Pid} = esque_consumer:start_consumer(?TEST_GROUP, ?TEST_TOPIC, ?TEST_PARTITION),
    {State, #state{topic=Topic, partition=Partition, group=Group, last_offset=LastOffset}} = sys:get_state(Pid),
    ?assertEqual(listen, State), %check the consumer goes into the listening state
    ?assertEqual(?TEST_TOPIC, Topic), %check the consumer is listening to the correct topic
    ?assertEqual(?TEST_GROUP, Group), %check the consumer is in the correct group
    ?assertEqual(-1, LastOffset), %as the topic has no message this last offset is -1
    ?assertEqual(?TEST_PARTITION, Partition). 

start_and_receive() ->
    {ok, Pid} = esque_consumer:start_consumer(?TEST_GROUP, ?TEST_TOPIC, ?TEST_PARTITION),
    esque_qs:put(?TEST_TOPIC, 0, <<"key">>, <<"value">>),
    {_State, #state{last_offset=LastOffset}} = sys:get_state(Pid),
    ?assertEqual(0, LastOffset). %as we have put 1 msg the last offset in the consumer should be 0. i.e we have recieved the 0th msg

    



    

