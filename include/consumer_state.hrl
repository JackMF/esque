-record(state, {
    group :: binary(),
    topic :: atom(),
    partition :: non_neg_integer(),
    next_offset_to_send_from :: non_neg_integer()
 }).