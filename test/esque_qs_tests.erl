-module(esque_qs_tests).
-include_lib("eunit/include/eunit.hrl").




test_setup() ->
    esque_qs:init(),
    esque_reg:init().

test_teardown(_) ->
    esque_reg:shutdown(),
    esque_qs:shutdown().

all_test_() ->
    {
        setup,
        fun test_setup/0,
        fun test_teardown/1,
        [
            {"sharding tables", fun shard/0},
            {"new table test", fun new/0},
            {"put item in q", fun put/0},
            {"two messages with same key", fun put_twice/0},
            {"all message from offset", fun all_messages_from_offset/0},
            {"all message from no existing offset", fun all_from_non_existing_offset/0}

        ]
    }.

shard() ->
    %Checking the sharding logic works
    ?assertEqual(table_name_0, esque_qs:shard(table_name, 0)),
    ?assertEqual(table_name_1, esque_qs:shard(table_name, 01)),
    ?assertEqual('table_name_-1', esque_qs:shard(table_name, -1)).

new() ->
    %When we create a q with three partitions....
    esque_qs:new(q_name, 3),

    %...we should make 3 ets tables corresponding to the "store"
    [?assertEqual([], ets:tab2list(q_name_0)),
     ?assertEqual([], ets:tab2list(q_name_1)),
     ?assertEqual([], ets:tab2list(q_name_2))
     ],

     esque_qs:delete(q_name, 3).
    

put() ->
    %Making a q with one partition
    esque_qs:new(q_name, 1),

    %When we put into a q a key and value...
    esque_qs:put(q_name, 0, <<"some_key1">>, #{1 => 2}),

    %We should find it in the store ets table...
    ?assertEqual({<<"some_key1">>, #{1 => 2}, 0}, esque_qs:lookup_store_by_key(q_name, 0, <<"some_key1">>)),

    % ... and have an entry for it in the log dets table.
    ?assertEqual({0, <<"some_key1">>, #{1 => 2}}, esque_qs:lookup_log_by_offset(q_name, 0, 0)),

    esque_qs:delete(q_name, 1).



put_twice() ->
    %Making a q with one partition
    esque_qs:new(q_name, 1),
    esque_qs:put(q_name, 0, <<"some_key1">>, #{1 => 2}),
    esque_qs:put(q_name, 0, <<"some_key1">>, #{2 => 3}),

    % ... and have an entry for it in the log dets table.
    ?assertEqual({<<"some_key1">>, #{1 => 2, 2=>3}, 1}, esque_qs:lookup_store_by_key(q_name, 0, <<"some_key1">>)),

    esque_qs:delete(q_name, 1).

all_messages_from_offset() ->
    esque_qs:new(q_name, 1),

    esque_qs:put(q_name, 0, <<"some_key1">>, <<"some_value1">>),
    esque_qs:put(q_name, 0, <<"some_key2">>, <<"some_value2">>),

    ?assertEqual([{0, <<"some_key1">>, <<"some_value1">>},
                  {1, <<"some_key2">>, <<"some_value2">>}
                  ], esque_qs:all_messages_from_offset(q_name, 0, 0)),

    ?assertEqual([
                  {1, <<"some_key2">>, <<"some_value2">>}
                  ], esque_qs:all_messages_from_offset(q_name, 0, 1)),
    esque_qs:delete(q_name, 1).

all_from_non_existing_offset() ->
    esque_qs:new(q_name, 1),

    esque_qs:put(q_name, 0, <<"some_key1">>, <<"some_value1">>),
    esque_qs:put(q_name, 0, <<"some_key2">>, <<"some_value2">>),

    ?assertEqual([], esque_qs:all_messages_from_offset(q_name, 0, 3)),
    ?assertEqual([], esque_qs:all_messages_from_offset(q_name, 0, -1)),

    esque_qs:delete(q_name, 1).