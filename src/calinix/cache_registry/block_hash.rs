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
    let mut bytes = Vec::new();
    for token in tokens {
        bytes.extend_from_slice(token.as_bytes());
        bytes.push(0xff);
    }
    fnv1a64(&bytes)
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

#[cfg(test)]
mod tests {

    use super::{hash_block, prompt_to_block_hashes, tokenize, DEFAULT_BLOCK_SIZE};

    #[test]
    fn prompt_is_split_into_configured_token_blocks() {
        let hashes = prompt_to_block_hashes("one two three four five", None);

        assert_eq!(DEFAULT_BLOCK_SIZE, 4);
        assert_eq!(hashes.len(), 2);
        assert_ne!(hashes[0], hashes[1]);
    }

    #[test]
    fn testing() {
        let prompt = "Explain the kuberenetes";
        let block_size = 3;

        let tokenize = tokenize(prompt);
        let block_split = tokenize.chunks(block_size);
        let block_hashes: Vec<_> = block_split.map(hash_block).collect();

        println!("tokenize={:?}", tokenize);
        println!("block_hashes={:?}", block_hashes);
    }
}
