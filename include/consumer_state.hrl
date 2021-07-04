-record(state, {
    group :: binary(),
    topic :: atom(),
    partition :: non_neg_integer(),
    last_offset :: non_neg_integer()
 }).