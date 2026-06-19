use super::block_hash::BlockHash;

pub const DEFAULT_SHARD_COUNT: usize = 256;
pub const FIBONACCI: u64 = 0x9E37_79B9_7F4A_7C15;

pub fn shard_for_fibonacci(hash: BlockHash) -> usize {
    shard_for_fibonacci_with_count(hash, DEFAULT_SHARD_COUNT)
}

pub fn shard_for_fibonacci_with_count(hash: BlockHash, shard_count: usize) -> usize {
    let shard_count = shard_count.max(1);
    let mixed = hash.wrapping_mul(FIBONACCI);
    ((mixed as u128 * shard_count as u128) >> 64) as usize
}

pub fn shard_for_low_bits(hash: BlockHash) -> usize {
    shard_for_low_bits_with_count(hash, DEFAULT_SHARD_COUNT)
}

pub fn shard_for_low_bits_with_count(hash: BlockHash, shard_count: usize) -> usize {
    let shard_count = shard_count.max(1);
    (hash as usize) % shard_count
}

pub fn shard_for(hash: BlockHash) -> usize {
    shard_for_fibonacci(hash)
}

pub fn shard_for_with_count(hash: BlockHash, shard_count: usize) -> usize {
    shard_for_fibonacci_with_count(hash, shard_count)
}

#[cfg(test)]
mod tests {
    use super::{shard_for_with_count, DEFAULT_SHARD_COUNT};

    #[test]
    fn shard_is_inside_runtime_range() {
        assert!(shard_for_with_count(42, DEFAULT_SHARD_COUNT) < DEFAULT_SHARD_COUNT);
        assert!(shard_for_with_count(u64::MAX, 17) < 17);
    }
}
