-module(esque_qs_tests).
-include_lib("eunit/include/eunit.hrl").




test_setup() ->
	esque_reg:init().

test_teardown(_) ->
	esque_reg:shutdown().

all_test_() ->
    {
        setup,
        fun test_setup/0,
        fun test_teardown/1,
        [
            % Simplest possible. One single assertion.
            % Note the presence of the "_" prefix.
			{"sharding tables", fun shard/0},
			{"new table test", fun new/0},
			{"put item in q", fun put/0},
			{"get all messages from an offset", fun all_messages_from_offset/0},
			{"get all messages from an offset which doesnt exist", fun all_from_non_existing_offset/0}

        ]
    }.



shard() ->
	?assertEqual(table_name_0, esque_qs:shard(table_name, 0)),
	?assertEqual(table_name_1, esque_qs:shard(table_name, 01)),
	?assertEqual('table_name_-1', esque_qs:shard(table_name, -1)).


new() ->
	esque_qs:new(q_name, 3),

	[?assertEqual([], ets:tab2list(q_name_0)),
	 ?assertEqual([], ets:tab2list(q_name_1)),
	 ?assertEqual([], ets:tab2list(q_name_2))
	 ],

	ets:delete(q_name_0),
	ets:delete(q_name_1),
	ets:delete(q_name_2).

put() ->
	esque_qs:new(q_name, 1),

	esque_qs:put(q_name, 0, <<"some_key1">>, <<"some_value1">>),
	?assertEqual([{0, <<"some_key1">>, <<"some_value1">>}], ets:lookup(q_name_0, 0)),


	esque_qs:put(q_name, 0, <<"some_key2">>, <<"some_value2">>),
	?assertEqual([{1, <<"some_key2">>, <<"some_value2">>}], ets:lookup(q_name_0, 1)),

	ets:delete(q_name_0).

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
	ets:delete(q_name_0).

all_from_non_existing_offset() ->
	esque_qs:new(q_name, 1),

	esque_qs:put(q_name, 0, <<"some_key1">>, <<"some_value1">>),
	esque_qs:put(q_name, 0, <<"some_key2">>, <<"some_value2">>),

	?assertEqual([], esque_qs:all_messages_from_offset(q_name, 0, 3)),
	?assertEqual([], esque_qs:all_messages_from_offset(q_name, 0, -1)).
	
