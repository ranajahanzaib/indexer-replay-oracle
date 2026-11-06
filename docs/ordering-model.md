# Ordering model

Canonical source order is `(block_number, transaction_index, log_index)`. Identity is a separate block/transaction/log tuple. Strict mode adds edges between each adjacent canonical event, permitting retries but no source permutation.

`conflict_keys` is an explicit assertion by the application owner that events with disjoint keys can commute. Every event must have nonempty unique keys. For each key, an edge connects each occurrence to the next source occurrence. Explicit event-ID dependencies add further edges. The union must be acyclic and canonical source order must be a valid topological order.

Generation uses ChaCha8Rng from rand_chacha 0.3.1 with `seed_from_u64(testing.seed.wrapping_add(scenario_index))`. Ready sets are sorted source indices; choose uniformly using rand 0.8 when reordering is enabled. No hash iteration order or clock affects generation. Version and lockfile matter to seed mapping. Persisted schedules remain the primary exact reproduction mechanism.

Only **first** deliveries must respect all graph predecessors. Later deliveries are retries and may arrive after dependent events or across process restarts. This models delayed redelivery; applications cannot assume deduplication occurs immediately. No source identity is invented, and every complete scenario delivers every canonical event at least once. `max_duplicates_per_event` bounds explicit retries; a crash operation additionally entails one recorded recovery redelivery. Restart count bounds explicit restart operations; crash recovery itself necessarily relaunches a process.

Named scenarios exercise exact replay, immediate/delayed duplicates, restarts, a deterministic crash, and reordering separately. Seeded mixed schedules choose topological first delivery order, immediate duplicates, restart placements, one crash hook when enabled, and a delayed retry. Counts/operations are reported, rather than claiming exhaustive schedules. A sampled topological order may coincide with canonical order; inspect the trace for actual coverage. Multiple runs improve sampling without proving all legal orders were tested.
