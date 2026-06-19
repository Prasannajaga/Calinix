use super::block_hash::{
    fnv1a64, prompt_to_block_hashes, prompt_to_block_hashes_with_size, BlockHash,
};

pub fn combine_cumulative(prev: BlockHash, block_hash: BlockHash) -> BlockHash {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&prev.to_le_bytes());
    bytes[8..].copy_from_slice(&block_hash.to_le_bytes());
    fnv1a64(&bytes)
}

pub fn cumulative_hashes_from_blocks(block_hashes: &[BlockHash]) -> Vec<BlockHash> {
    let mut cumulative = Vec::with_capacity(block_hashes.len());
    let mut prev = 0;
    for block_hash in block_hashes {
        prev = combine_cumulative(prev, *block_hash);
        cumulative.push(prev);
    }
    cumulative
}

pub fn prompt_to_cumulative_hashes(prompt: &str) -> Vec<BlockHash> {
    let block_hashes = prompt_to_block_hashes(prompt, None);
    cumulative_hashes_from_blocks(&block_hashes)
}

pub fn prompt_to_cumulative_hashes_with_block_size(
    prompt: &str,
    block_size: usize,
) -> Vec<BlockHash> {
    let block_hashes = prompt_to_block_hashes_with_size(prompt, block_size);
    cumulative_hashes_from_blocks(&block_hashes)
}

pub fn make_synthetic_chain(chain_id: u64, blocks: usize) -> Vec<BlockHash> {
    let mut out = Vec::with_capacity(blocks);
    let mut prev = 0;
    for block_index in 0..blocks {
        let local = fnv1a64(format!("chain={chain_id}:block={block_index}").as_bytes());
        let cumulative = combine_cumulative(prev, local);
        out.push(cumulative);
        prev = cumulative;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::prompt_to_cumulative_hashes;

    #[test]
    fn cumulative_hashes_change_with_prefix_order() {
        let first = prompt_to_cumulative_hashes("one two three four five six");
        let second = prompt_to_cumulative_hashes("five six one two three four");

        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        assert_ne!(first, second);
    }
}
