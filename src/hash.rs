pub const BLOCK_SIZE: usize = 4;
const FNV_OFFSET: u64 = 14695981039346656037;
const FNV_PRIME: u64 = 1099511628211;

pub type BlockHash = u64;

pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

pub fn tokenize(prompt: &str) -> Vec<String> {
    prompt
        .split_whitespace()
        .map(|token| token.to_string())
        .collect()
}

pub fn hash_block(tokens: &[String]) -> BlockHash {
    let mut bytes = Vec::new();
    for token in tokens {
        bytes.extend_from_slice(token.as_bytes());
        bytes.push(0xff);
    }
    fnv1a64(&bytes)
}

pub fn combine_cumulative(prev: BlockHash, block_hash: BlockHash) -> BlockHash {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&prev.to_le_bytes());
    bytes[8..].copy_from_slice(&block_hash.to_le_bytes());
    fnv1a64(&bytes)
}

pub fn prompt_to_block_hashes(prompt: &str) -> Vec<BlockHash> {
    tokenize(prompt)
        .chunks(BLOCK_SIZE)
        .map(hash_block)
        .collect()
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
    let block_hashes = prompt_to_block_hashes(prompt);
    cumulative_hashes_from_blocks(&block_hashes)
}

pub fn make_synthetic_chain(chain_id: u64, blocks: usize) -> Vec<BlockHash> {
    let mut out = Vec::with_capacity(blocks);
    let mut prev = FNV_OFFSET;
    for i in 0..blocks {
        let local = fnv1a64(format!("chain={chain_id}:block={i}").as_bytes());
        let cumulative = combine_cumulative(prev, local);
        out.push(cumulative);
        prev = cumulative;
    }
    out
}
