use super::block_hash::{fnv1a64, BlockHash, DEFAULT_BLOCK_SIZE};

const FNV_OFFSET: u64 = 14_695_981_039_346_656_037;
const FNV_PRIME: u64 = 1_099_511_628_211;

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
    prompt_to_cumulative_hashes_streaming(prompt, DEFAULT_BLOCK_SIZE)
}

pub fn prompt_to_cumulative_hashes_with_block_size(
    prompt: &str,
    block_size: usize,
) -> Vec<BlockHash> {
    prompt_to_cumulative_hashes_streaming(prompt, block_size)
}

pub fn prompt_to_cumulative_hashes_streaming(
    prompt: &str,
    block_size: usize,
) -> Vec<BlockHash> {
    let block_size = block_size.max(1);
    let mut cumulative = Vec::new();
    let mut prev_cumulative: BlockHash = 0;
    let mut block_hash = FNV_OFFSET;
    let mut tokens_in_block = 0;

    for token in prompt.split_whitespace() {
        for byte in token.as_bytes() {
            block_hash ^= *byte as u64;
            block_hash = block_hash.wrapping_mul(FNV_PRIME);
        }
        block_hash ^= 0xff;
        block_hash = block_hash.wrapping_mul(FNV_PRIME);
        tokens_in_block += 1;

        if tokens_in_block == block_size {
            prev_cumulative = combine_cumulative(prev_cumulative, block_hash);
            cumulative.push(prev_cumulative);
            block_hash = FNV_OFFSET;
            tokens_in_block = 0;
        }
    }

    if tokens_in_block > 0 {
        prev_cumulative = combine_cumulative(prev_cumulative, block_hash);
        cumulative.push(prev_cumulative);
    }

    cumulative
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
    use super::{prompt_to_cumulative_hashes, prompt_to_cumulative_hashes_streaming, cumulative_hashes_from_blocks};
    use crate::cache_registry::block_hash::prompt_to_block_hashes_with_size;

    #[test]
    fn cumulative_hashes_change_with_prefix_order() {
        let first = prompt_to_cumulative_hashes("one two three four five six");
        let second = prompt_to_cumulative_hashes("five six one two three four");

        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        assert_ne!(first, second);
    }

    #[test]
    fn streaming_matches_original_behavior() {
        let prompt = "one two three four five six seven eight nine ten eleven twelve";
        let block_size = 3;
        
        // original
        let blocks = prompt_to_block_hashes_with_size(prompt, block_size);
        let orig = cumulative_hashes_from_blocks(&blocks);
        
        // streaming
        let stream = prompt_to_cumulative_hashes_streaming(prompt, block_size);
        
        assert_eq!(orig, stream);
    }
}
