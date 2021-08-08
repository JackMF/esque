-module(esque_merge).


-export([deep_merge/2]).

-spec deep_merge(M1 :: map(), M2 :: map()) -> map().
deep_merge(M1, M2) when is_map(M1), is_map(M2) ->
    maps:fold(
        fun(K, V2, Acc) ->
            case Acc of
                #{K := V1} ->
                    Acc#{K => deep_merge(V1, V2)};
                _ ->
                    Acc#{K => V2}
            end
        end, M1, M2);
deep_merge(_, Override) ->
    Override.