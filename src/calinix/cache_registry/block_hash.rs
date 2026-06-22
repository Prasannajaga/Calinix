pub const DEFAULT_BLOCK_SIZE: usize = 4;

const FNV_OFFSET: u64 = 14_695_981_039_346_656_037;
const FNV_PRIME: u64 = 1_099_511_628_211;

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
    let mut hash = FNV_OFFSET;
    for token in tokens {
        for byte in token.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

pub fn prompt_to_block_hashes(prompt: &str, block_size: Option<usize>) -> Vec<BlockHash> {
    let size = block_size.unwrap_or(DEFAULT_BLOCK_SIZE);
    prompt_to_block_hashes_with_size(prompt, size)
}

pub fn prompt_to_block_hashes_with_size(prompt: &str, block_size: usize) -> Vec<BlockHash> {
    let block_size = block_size.max(1);
    tokenize(prompt)
        .chunks(block_size)
        .map(hash_block)
        .collect()
}

pub fn prompt_to_token_blocks_with_size(prompt: &str, block_size: usize) -> Vec<String> {
    let block_size: usize = block_size.max(1);
    tokenize(prompt)
        .chunks(block_size)
        .map(|tokens| tokens.join(" "))
        .collect()
}


