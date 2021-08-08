-module(esque_merge_tests).
-include_lib("eunit/include/eunit.hrl").






deep_merge_0_test() ->
    Map1 = 
        #{
            first_name => "jack",
            address => #{
                number => 11,
                street => "made up street"
            }
        },
    Map2 =
        #{last_name => "fletcher"
        },

    ?assertEqual(#{
        first_name => "jack",
        last_name => "fletcher",
        address => #{
            number => 11,
            street => "made up street"
        }
    }, esque_merge:deep_merge(Map1, Map2)).
deep_merge_1_test() ->
    Map1 = 
        #{
            first_name => "jack",
            address => #{
                number => 11,
                street => "made up street"
            }
        },
    Map2 =
        #{address => #{town => "edinburgh"}
        },

    ?assertEqual(#{
        first_name => "jack",
        address => #{
            number => 11,
            street => "made up street",
            town => "edinburgh"
        }
    }, esque_merge:deep_merge(Map1, Map2)).