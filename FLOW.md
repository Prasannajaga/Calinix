# Bitmap + Sharded Block Indexer Explanation

This README explains the concepts discussed in this chat:

1. What `HostBitmap` is
2. Why `HostBitmap { words: [7, 0, 0, 0] }` means pods `[0, 1, 2]`
3. Where `words` gets set
4. What binary bits mean
5. What happens if you need more than 256 pods
6. What the sharded indexer is used for
7. How the binary-search-style prefix matching loop works

---

# 1. HostBitmap Purpose

`HostBitmap` is a compact way to store pod IDs.

Instead of storing pod IDs like this:

    [0, 1, 2]

it stores them as bits inside integers:

    HostBitmap { words: [7, 0, 0, 0] }

Each bit represents one pod.

If a bit is `1`, that pod is included.
If a bit is `0`, that pod is not included.

---

# 2. HostBitmap Structure

Code:

    pub const MAX_PODS: usize = 256;
    const WORDS: usize = 4;
    const BITS_PER_WORD: usize = 64;

    pub struct HostBitmap {
        words: [u64; WORDS],
    }

Meaning:

    words[0] stores pods 0..63
    words[1] stores pods 64..127
    words[2] stores pods 128..191
    words[3] stores pods 192..255

Each `u64` has 64 bits.

So total capacity is:

    4 words * 64 bits = 256 pods

That is why `MAX_PODS` is 256.

---

# 3. Why words: [7, 0, 0, 0] Means Pods [0, 1, 2]

You ran:

    let candidates = HostBitmap::full_for_count(3);
    println!("hashes: {:?}", candidates);
    println!("candidates: {:?}", candidates.iter_set_bits());

Output:

    hashes: HostBitmap { words: [7, 0, 0, 0] }
    candidates: [0, 1, 2]

These are not different values.

They are two different views of the same data.

`words[0] = 7`

Decimal `7` in binary is:

    111

Read bits from right to left:

    bit index:  2 1 0
    binary:     1 1 1

That means:

    bit 0 = 1 => pod 0 is set
    bit 1 = 1 => pod 1 is set
    bit 2 = 1 => pod 2 is set

So:

    words[0] = 7

means:

    pods [0, 1, 2] are set

---

# 4. Important: Rightmost Bit Is Pod 0

This was the confusing part.

Binary:

    0111

does NOT mean pod 0 is dead.

Read it like this:

    bit index:  3 2 1 0
    binary:     0 1 1 1

So:

    bit 0 = 1 => pod 0 is set
    bit 1 = 1 => pod 1 is set
    bit 2 = 1 => pod 2 is set
    bit 3 = 0 => pod 3 is not set

The rightmost bit is pod 0.

So:

    0111

means:

    pods [0, 1, 2] are active/set/included

It does not mean pod 0 is dead.

---

# 5. Where words Gets Set

You call:

    HostBitmap::full_for_count(3)

That function is:

    pub fn full_for_count(count: usize) -> Self {
        let mut bitmap = Self::empty();

        for pod_id in 0..count.min(MAX_PODS) {
            bitmap.set(pod_id);
        }

        bitmap
    }

So for count `3`, the loop runs:

    pod_id = 0
    pod_id = 1
    pod_id = 2

Each time it calls:

    bitmap.set(pod_id);

The actual mutation happens inside `set()`:

    pub fn set(&mut self, pod_id: usize) {
        if pod_id >= MAX_PODS {
            return;
        }

        self.words[pod_id / BITS_PER_WORD] |=
            1_u64 << (pod_id % BITS_PER_WORD);
    }

This line changes `words`:

    self.words[pod_id / BITS_PER_WORD] |= 1_u64 << (pod_id % BITS_PER_WORD);

---

# 6. Step-by-Step: full_for_count(3)

Start:

    words = [0, 0, 0, 0]

Binary:

    words[0] = 0000

---

## Step 1: set(0)

Code:

    self.words[0 / 64] |= 1_u64 << (0 % 64)

Simplifies to:

    self.words[0] |= 1 << 0

`1 << 0` is:

    binary: 0001
    decimal: 1

So:

    words[0] = 0 | 1
    words[0] = 1

Now:

    words[0] = 0001

Meaning:

    pod 0 is set

---

## Step 2: set(1)

Code:

    self.words[1 / 64] |= 1_u64 << (1 % 64)

Simplifies to:

    self.words[0] |= 1 << 1

`1 << 1` is:

    binary: 0010
    decimal: 2

Current:

    words[0] = 0001

OR with:

    0010

Result:

    0001
    0010
    ----
    0011

Decimal `0011` is `3`.

Now:

    words[0] = 3

Meaning:

    pod 0 is set
    pod 1 is set

---

## Step 3: set(2)

Code:

    self.words[2 / 64] |= 1_u64 << (2 % 64)

Simplifies to:

    self.words[0] |= 1 << 2

`1 << 2` is:

    binary: 0100
    decimal: 4

Current:

    words[0] = 0011

OR with:

    0100

Result:

    0011
    0100
    ----
    0111

Decimal `0111` is `7`.

Final:

    words[0] = 7

So:

    words = [7, 0, 0, 0]

Meaning:

    pods [0, 1, 2] are set

---

# 7. What `|=` Means

This:

    self.words[0] |= 4

means:

    self.words[0] = self.words[0] | 4

`|` is bitwise OR.

It turns on a bit without removing existing bits.

Example:

    current value: 0011
    new bit:       0100
    result:        0111

So if pods 0 and 1 are already set, setting pod 2 keeps pods 0 and 1.

---

# 8. Pod ID to Bit Mapping

Each pod maps to one bit.

For the first few pods:

    pod 0 => bit 0 => value 1
    pod 1 => bit 1 => value 2
    pod 2 => bit 2 => value 4
    pod 3 => bit 3 => value 8
    pod 4 => bit 4 => value 16

So:

    pod 0 + pod 1 + pod 2

means:

    1 + 2 + 4 = 7

That is why pods `[0, 1, 2]` become:

    words[0] = 7

---

# 9. More Examples

## Example: Only Pod 1 and Pod 3 Are Set

Bits:

    bit index:  3 2 1 0
    binary:     1 0 1 0

This is:

    1010 binary = 10 decimal

So bitmap would print:

    HostBitmap { words: [10, 0, 0, 0] }

And:

    iter_set_bits()

would return:

    [1, 3]

Because bit 1 and bit 3 are set.

---

## Example: Pod 64

Pod 64 does not go into `words[0]`.

Because:

    pod_id / 64 = 64 / 64 = 1

So pod 64 goes into:

    words[1]

Its bit inside that word is:

    pod_id % 64 = 64 % 64 = 0

So pod 64 is:

    words[1], bit 0

---

# 10. What Happens If You Need More Than 256 Pods?

Current capacity:

    WORDS = 4
    BITS_PER_WORD = 64

So:

    4 * 64 = 256 pods

Valid pod IDs are:

    0..255

If you call:

    bitmap.set(256);
    bitmap.set(300);

nothing happens because of this check:

    if pod_id >= MAX_PODS {
        return;
    }

---

# 11. Supporting More Pods

## Option 1: Increase Fixed Capacity

For 512 pods:

    pub const MAX_PODS: usize = 512;
    const WORDS: usize = 8;
    const BITS_PER_WORD: usize = 64;

Because:

    512 / 64 = 8 words

Mapping:

    words[0] => pods 0..63
    words[1] => pods 64..127
    words[2] => pods 128..191
    words[3] => pods 192..255
    words[4] => pods 256..319
    words[5] => pods 320..383
    words[6] => pods 384..447
    words[7] => pods 448..511

---

## Option 2: Calculate WORDS Automatically

Better:

    pub const MAX_PODS: usize = 512;
    const BITS_PER_WORD: usize = 64;
    const WORDS: usize = (MAX_PODS + BITS_PER_WORD - 1) / BITS_PER_WORD;

Examples:

    MAX_PODS = 256  => WORDS = 4
    MAX_PODS = 512  => WORDS = 8
    MAX_PODS = 1000 => WORDS = 16

---

## Option 3: Dynamic Bitmap

If pod count is not fixed, use:

    pub struct HostBitmap {
        words: Vec<u64>,
    }

Then resize the vector as needed.

Current version:

    fixed-size bitmap

Dynamic version:

    growable bitmap

---

# 12. ShardedBlockIndexer Purpose

The main purpose of `ShardedBlockIndexer` is:

    Given a block hash, quickly find which pods have that hash.

Conceptually it stores:

    BlockHash -> HostBitmap of pod IDs

Example:

    hash_A -> pods [0, 2, 5]
    hash_B -> pods [1, 3]
    hash_C -> pods [0, 1, 2]

In Rust, that is:

    HashMap<BlockHash, HostBitmap>

But instead of one big `HashMap`, the code uses 256 smaller hash maps called shards.

---

# 13. ShardedBlockIndexer Structure

Code:

    pub struct ShardedBlockIndexer {
        shards: Vec<RwLock<HashMap<BlockHash, HostBitmap>>>,
        alive: RwLock<HostBitmap>,
    }

Meaning:

    shards = 256 small hash maps
    alive  = bitmap of currently alive pods

Each shard is protected by an `RwLock`.

So multiple readers can read at the same time, but writes need exclusive access.

---

# 14. Why Use Shards?

Without sharding, you might have:

    RwLock<HashMap<BlockHash, HostBitmap>>

That means every lookup, insert, and delete fights for the same lock.

With sharding:

    Vec<RwLock<HashMap<BlockHash, HostBitmap>>>

Only one shard is locked.

So if two hashes belong to different shards, they can be accessed in parallel.

Main benefit:

    less lock contention
    better concurrency
    faster access under load

---

# 15. Shard Constants

Code:

    pub const SHARDS: usize = 256;
    pub const SHARD_BITS: u32 = 8;

256 shards means shard IDs are:

    0..255

8 bits can represent 256 values:

    2^8 = 256

That is why `SHARD_BITS` is 8.

---

# 16. shard_for_fibonacci

Code:

    pub const FIBONACCI: u64 = 0x9E3779B97F4A7C15;

    pub fn shard_for_fibonacci(hash: BlockHash) -> usize {
        ((hash.wrapping_mul(FIBONACCI)) >> (64 - SHARD_BITS)) as usize
    }

Purpose:

    Convert a BlockHash into a shard number from 0 to 255.

Example:

    hash_1 -> shard 17
    hash_2 -> shard 201
    hash_3 -> shard 17
    hash_4 -> shard 99

Hashes in the same shard go into the same smaller HashMap.

The Fibonacci multiplication helps spread hashes across shards better than simply using low bits.

---

# 17. register()

Code:

    pub fn register(&self, pod_id: usize, cumulative_hash: BlockHash) {
        let shard = shard_for_fibonacci(cumulative_hash);
        let mut guard = self.shards[shard].write().expect("index shard poisoned");

        guard
            .entry(cumulative_hash)
            .or_insert_with(HostBitmap::empty)
            .set(pod_id);
    }

Purpose:

    Mark that a pod owns/has a hash.

Example:

    register(2, hash_A)

Means:

    pod 2 has hash_A

Before:

    hash_A -> []

After:

    hash_A -> [2]

Then:

    register(5, hash_A)

After:

    hash_A -> [2, 5]

Internally, `[2, 5]` is stored as a bitmap.

---

# 18. evict()

Code:

    pub fn evict(&self, pod_id: usize, cumulative_hash: BlockHash) {
        let shard = shard_for_fibonacci(cumulative_hash);
        let mut guard = self.shards[shard].write().expect("index shard poisoned");

        if let Some(owners) = guard.get_mut(&cumulative_hash) {
            owners.clear(pod_id);

            if owners.is_empty() {
                guard.remove(&cumulative_hash);
            }
        }
    }

Purpose:

    Remove a pod from the owners of a hash.

Example before:

    hash_A -> [2, 5]

Call:

    evict(2, hash_A)

After:

    hash_A -> [5]

If no pods remain:

    hash_A -> []

then the hash entry is removed from the map.

This avoids storing useless empty entries.

---

# 19. shutdown()

Code:

    pub fn shutdown(&self, pod_id: usize) {
        self.alive
            .write()
            .expect("alive bitmap poisoned")
            .clear(pod_id);
    }

Purpose:

    Mark a pod as dead/offline.

Example:

    alive = [0, 1, 2]

Call:

    shutdown(1)

After:

    alive = [0, 2]

Important:

    shutdown() does not remove pod 1 from every hash entry immediately.

It only marks pod 1 as not alive.

---

# 20. owners()

Code:

    pub fn owners(&self, cumulative_hash: BlockHash) -> HostBitmap {
        let shard = shard_for_fibonacci(cumulative_hash);

        self.shards[shard]
            .read()
            .expect("index shard poisoned")
            .get(&cumulative_hash)
            .copied()
            .unwrap_or_else(HostBitmap::empty)
    }

Purpose:

    Return all pods that have this hash.

This includes alive and dead pods.

Example:

    hash_A -> [0, 1, 2]
    alive  -> [0, 2]

Then:

    owners(hash_A) = [0, 1, 2]

Pod 1 is still returned because `owners()` does not filter dead pods.

---

# 21. owners_alive()

Code:

    pub fn owners_alive(&self, cumulative_hash: BlockHash) -> HostBitmap {
        self.owners(cumulative_hash).and(self.alive())
    }

Purpose:

    Return only alive pods that have this hash.

Example:

    owners(hash_A) = [0, 1, 2]
    alive          = [0, 2]

AND result:

    owners_alive(hash_A) = [0, 2]

This line is important:

    self.owners(cumulative_hash).and(self.alive())

It performs bitmap intersection.

Meaning:

    keep only pods that are in both sets

---

# 22. cleanup_dead_pod()

Code:

    pub fn cleanup_dead_pod(&self, pod_id: usize) {
        for shard in &self.shards {
            let mut guard = shard.write().expect("index shard poisoned");

            guard.retain(|_, owners| {
                owners.clear(pod_id);
                !owners.is_empty()
            });
        }
    }

Purpose:

    Physically remove a dead pod from all hash entries.

Example before:

    hash_A -> [0, 1]
    hash_B -> [1, 2]
    hash_C -> [2]

Call:

    cleanup_dead_pod(1)

After:

    hash_A -> [0]
    hash_B -> [2]
    hash_C -> [2]

If an entry becomes empty, it is removed.

Example:

    hash_X -> [1]

After cleanup:

    hash_X -> []

So the whole `hash_X` entry is removed.

---

# 23. shard_entry_counts()

Code:

    pub fn shard_entry_counts(&self) -> Vec<usize> {
        self.shards
            .iter()
            .map(|shard| shard.read().expect("index shard poisoned").len())
            .collect()
    }

Purpose:

    Return how many hash entries are stored in each shard.

Example output:

    [10, 12, 8, 20, ...]

Useful for debugging shard distribution.

If one shard has way more entries than others, hash distribution may be bad.

---

# 24. Prefix Matching Purpose

Function:

    longest_prefix_lengths_for_candidates(...)

Purpose:

    For each candidate pod, find how much of a prefix it matches.

Imagine a target has cumulative hashes:

    depth 1 -> H1
    depth 2 -> H2
    depth 3 -> H3
    depth 4 -> H4

And pods have:

    pod 0 has H1, H2
    pod 1 has H1
    pod 2 has H1, H2, H3, H4

Then result is:

    pod 0 -> 2
    pod 1 -> 1
    pod 2 -> 4

Meaning:

    pod 0 matches prefix length 2
    pod 1 matches prefix length 1
    pod 2 matches prefix length 4

---

# 25. SearchFrame

Code:

    pub struct SearchFrame {
        min_prefix_depth: usize,
        max_prefix_depth: usize,
        candidate_pods: HostBitmap,
    }

Meaning:

    For these candidate pods, their answer is somewhere between min and max.

Example:

    min_prefix_depth = 0
    max_prefix_depth = 4
    candidate_pods = [0, 1, 2]

Means:

    pods [0, 1, 2] have match length somewhere from 0 to 4.

---

# 26. Why This Uses a Stack

The algorithm uses a stack instead of recursion.

Code:

    let mut stack = Vec::with_capacity(...);

    stack.push(SearchFrame {
        min_prefix_depth: 0,
        max_prefix_depth: cumulative_hashes.len(),
        candidate_pods: candidate_pods.and(indexer.alive()),
    });

Then:

    while let Some(frame) = stack.pop() {
        ...
    }

This means:

    keep processing search ranges until there are none left.

Each frame splits into two smaller frames:

    pods that matched the probe depth
    pods that failed the probe depth

---

# 27. Binary Search Core Idea

Instead of checking every depth one by one:

    depth 1
    depth 2
    depth 3
    depth 4
    depth 5
    ...

The algorithm checks the middle depth first.

Example range:

    0..4

Probe:

    (0 + 4 + 1) / 2 = 2

So it checks depth 2.

Then it splits pods:

    pods that have depth 2    => search deeper
    pods that do not have it  => search shallower

This is like binary search, but for multiple pods at once.

---

# 28. Binary Loop Code

Core loop:

    while let Some(frame) = stack.pop() {
        if frame.candidate_pods.is_empty() {
            continue;
        }

        if frame.min_prefix_depth == frame.max_prefix_depth {
            for pod_id in frame.candidate_pods.iter_set_bits() {
                lengths[pod_id] = frame.min_prefix_depth;
            }
            continue;
        }

        let probe_prefix_depth =
            (frame.min_prefix_depth + frame.max_prefix_depth + 1) / 2;

        let pods_with_probe_prefix =
            indexer.owners_alive(cumulative_hashes[probe_prefix_depth - 1]);

        let pods_at_or_above_probe =
            frame.candidate_pods.and(pods_with_probe_prefix);

        let pods_below_probe =
            frame.candidate_pods.minus(pods_at_or_above_probe);

        stack.push(SearchFrame {
            min_prefix_depth: probe_prefix_depth,
            max_prefix_depth: frame.max_prefix_depth,
            candidate_pods: pods_at_or_above_probe,
        });

        stack.push(SearchFrame {
            min_prefix_depth: frame.min_prefix_depth,
            max_prefix_depth: probe_prefix_depth - 1,
            candidate_pods: pods_below_probe,
        });
    }

---

# 29. Binary Loop Example

Assume cumulative hashes:

    depth 1 -> H1
    depth 2 -> H2
    depth 3 -> H3
    depth 4 -> H4

Candidate pods:

    [0, 1, 2]

Index contains:

    H1 -> [0, 1, 2]
    H2 -> [0, 2]
    H3 -> [2]
    H4 -> [2]

Expected final result:

    pod 0 -> 2
    pod 1 -> 1
    pod 2 -> 4

---

# 30. Step 1: Initial Frame

Start frame:

    range: 0..4
    pods: [0, 1, 2]

Meaning:

    For pods [0, 1, 2], answer is between 0 and 4.

Stack:

    [range 0..4, pods [0, 1, 2]]

---

# 31. Step 2: Process Range 0..4

Calculate probe:

    probe = (0 + 4 + 1) / 2
          = 5 / 2
          = 2

So we test depth 2.

Depth 2 means:

    cumulative_hashes[2 - 1]
    cumulative_hashes[1]
    H2

Ask indexer:

    owners_alive(H2)

From our example:

    H2 -> [0, 2]

So:

    pods_with_probe_prefix = [0, 2]

Now split current pods:

    candidate_pods = [0, 1, 2]
    owners(H2)     = [0, 2]

Intersection:

    [0, 1, 2] AND [0, 2] = [0, 2]

So:

    pods_at_or_above_probe = [0, 2]

These pods matched depth 2.

Now find pods below probe:

    candidate_pods = [0, 1, 2]
    pods_at_or_above_probe = [0, 2]

Minus:

    [0, 1, 2] - [0, 2] = [1]

So:

    pods_below_probe = [1]

Meaning:

    pods [0, 2] matched depth 2, search deeper
    pod [1] failed depth 2, search shallower

Push frames:

    range 2..4, pods [0, 2]
    range 0..1, pods [1]

---

# 32. Step 3: Process Range 0..1 for Pod 1

Frame:

    range: 0..1
    pods: [1]

Calculate probe:

    probe = (0 + 1 + 1) / 2
          = 2 / 2
          = 1

Test depth 1.

Depth 1 means:

    H1

Index:

    H1 -> [0, 1, 2]

Current pods:

    [1]

Intersection:

    [1] AND [0, 1, 2] = [1]

So pod 1 matched depth 1.

Pods below probe:

    [1] - [1] = []

Push:

    range 1..1, pods [1]
    range 0..0, pods []

Empty frame gets ignored.

---

# 33. Step 4: Resolve Pod 1

Frame:

    range: 1..1
    pods: [1]

Since:

    min_prefix_depth == max_prefix_depth

the answer is known.

Set:

    lengths[1] = 1

So:

    pod 1 -> depth 1

---

# 34. Step 5: Process Range 2..4 for Pods 0 and 2

Frame:

    range: 2..4
    pods: [0, 2]

Calculate probe:

    probe = (2 + 4 + 1) / 2
          = 7 / 2
          = 3

Test depth 3.

Depth 3 means:

    H3

Index:

    H3 -> [2]

Current pods:

    [0, 2]

Intersection:

    [0, 2] AND [2] = [2]

So:

    pods_at_or_above_probe = [2]

Pods below probe:

    [0, 2] - [2] = [0]

Meaning:

    pod 2 matched depth 3, search deeper
    pod 0 failed depth 3, search shallower

Push:

    range 3..4, pods [2]
    range 2..2, pods [0]

---

# 35. Step 6: Resolve Pod 0

Frame:

    range: 2..2
    pods: [0]

Since min equals max:

    lengths[0] = 2

So:

    pod 0 -> depth 2

---

# 36. Step 7: Process Range 3..4 for Pod 2

Frame:

    range: 3..4
    pods: [2]

Calculate probe:

    probe = (3 + 4 + 1) / 2
          = 8 / 2
          = 4

Test depth 4.

Depth 4 means:

    H4

Index:

    H4 -> [2]

Current pods:

    [2]

Intersection:

    [2] AND [2] = [2]

So pod 2 matched depth 4.

Push:

    range 4..4, pods [2]
    range 3..3, pods []

Empty frame ignored.

---

# 37. Step 8: Resolve Pod 2

Frame:

    range: 4..4
    pods: [2]

Since min equals max:

    lengths[2] = 4

So:

    pod 2 -> depth 4

---

# 38. Final Prefix Result

Final `lengths`:

    lengths[0] = 2
    lengths[1] = 1
    lengths[2] = 4

Meaning:

    pod 0 matched prefix length 2
    pod 1 matched prefix length 1
    pod 2 matched prefix length 4

---

# 39. Important Lines in the Binary Loop

## Pick middle depth

    let probe_prefix_depth =
        (frame.min_prefix_depth + frame.max_prefix_depth + 1) / 2;

This picks the depth to test.

Example:

    range 0..4 => probe 2
    range 2..4 => probe 3
    range 3..4 => probe 4

The `+ 1` biases upward.

That prevents infinite loops when the range has two values.

Example:

    range 0..1

Without `+ 1`:

    (0 + 1) / 2 = 0

That could get stuck.

With `+ 1`:

    (0 + 1 + 1) / 2 = 1

It moves forward.

---

## Find pods with that prefix

    let pods_with_probe_prefix =
        indexer.owners_alive(cumulative_hashes[probe_prefix_depth - 1]);

If probe depth is 2, it looks at:

    cumulative_hashes[1]

Because arrays are zero-indexed.

Depth 1 is index 0.
Depth 2 is index 1.
Depth 3 is index 2.

---

## Pods that passed

    let pods_at_or_above_probe =
        frame.candidate_pods.and(pods_with_probe_prefix);

This means:

    current candidate pods
    AND pods that have the probe hash

So only pods that passed the probe remain.

---

## Pods that failed

    let pods_below_probe =
        frame.candidate_pods.minus(pods_at_or_above_probe);

This means:

    current pods - passed pods

Those pods failed the probe depth.

So their answer must be below the probe.

---

# 40. Whole Design Summary

The full system is designed like this:

    BlockHash -> HostBitmap of pod IDs

The bitmap stores pod IDs compactly using bits.

The sharded index stores hashes across 256 smaller maps.

The alive bitmap tracks which pods are currently alive.

The prefix matching code finds how much each pod matches a target prefix.

---

# 41. Why This Design Is Useful

## Fast owner lookup

You can quickly ask:

    Which pods have this hash?

Using:

    owners(hash)

---

## Fast alive filtering

You can quickly ask:

    Which alive pods have this hash?

Using:

    owners_alive(hash)

This is just bitmap AND:

    owners AND alive

---

## Fast pod removal

You can clear a pod bit quickly:

    owners.clear(pod_id)

---

## Fast concurrency

Sharding means different hashes can be accessed through different locks.

This reduces blocking.

---

## Fast prefix search

The binary-search-style loop avoids checking every prefix depth for every pod.

Instead, it splits pods into groups:

    pods that passed the current depth
    pods that failed the current depth

Then searches deeper or shallower.

---

# 42. Mental Model

Think of `HostBitmap` as a set of pods.

Example:

    [0, 1, 2]

is stored as:

    0111 binary

which is:

    7 decimal

So Rust prints:

    HostBitmap { words: [7, 0, 0, 0] }

But logically it means:

    pods 0, 1, and 2 are set

---

# 43. Mental Model for ShardedBlockIndexer

Think of it like this:

    Hash index:
        hash_A -> pods [0, 2]
        hash_B -> pods [1, 3]
        hash_C -> pods [0, 1, 2]

But internally split into shards:

    shard 0:
        hash_X -> pods [...]

    shard 1:
        hash_Y -> pods [...]

    shard 2:
        hash_Z -> pods [...]

This improves concurrency.

---

# 44. Mental Model for Prefix Matching

For each pod, the algorithm asks:

    How far does this pod match the target chain?

Instead of checking one by one, it asks:

    Does this pod match the middle depth?

If yes:

    search deeper

If no:

    search shallower

That gives each pod its longest matching prefix depth.

---

# 45. Final Key Points

1. `HostBitmap` stores pod IDs as bits.

2. `words[0] = 7` means binary `111`.

3. Binary `111` means pods `0`, `1`, and `2` are set.

4. Pod 0 is the rightmost bit.

5. `full_for_count(3)` calls `set(0)`, `set(1)`, and `set(2)`.

6. The line that modifies the bitmap is:

       self.words[pod_id / 64] |= 1_u64 << (pod_id % 64);

7. Current bitmap supports only 256 pods.

8. To support more pods, increase `WORDS` or use `Vec<u64>`.

9. `ShardedBlockIndexer` maps:

       BlockHash -> HostBitmap

10. Sharding reduces lock contention.

11. `alive` tracks which pods are alive.

12. `owners_alive(hash)` returns:

       owners(hash) AND alive

13. Prefix matching uses a binary-search-style algorithm.

14. Pods that pass a probe depth search deeper.

15. Pods that fail a probe depth search shallower.

16. Final result tells each pod's longest matching prefix length.