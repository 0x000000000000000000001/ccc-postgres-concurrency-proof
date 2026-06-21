# Verification

| Article claim | Test | Observed result | Proven or falsified |
| --- | --- | --- | --- |
| Under `READ COMMITTED`, combining the context check and insert into one atomic CTE does not prevent two overlapping appends from accepting the same expected context version. | `atomic_cte_allows_two_overlapping_appends_under_read_committed` | Both overlapping appends inserted rows; two matching `alice` events existed. | Proven |
| Establishing a global write order with a metadata row locked through `SELECT ... FOR UPDATE`, then recalculating the context version before inserting, makes concurrent appends for the same context produce one success and one context conflict. | `serialized_append_accepts_one_and_rejects_one_for_the_same_context` | One append succeeded; one returned a typed context conflict with expected `0` and actual equal to the winning sequence number; one matching `alice` event existed. | Proven |
| Establishing a global write order with a metadata row locked through `SELECT ... FOR UPDATE`, then recalculating the context version before inserting, allows concurrent appends for independent contexts to both succeed. | `serialized_append_accepts_both_for_independent_contexts` | Both appends succeeded; `alice` and `bob` each had one event; sequence numbers were distinct and metadata ended at `2`. | Proven |
