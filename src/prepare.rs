use crate::hash::{cumulative_hashes_from_blocks, hash_block, tokenize, BLOCK_SIZE};
use crate::types::{RequestContext, RoutingMode};

pub fn prepare(session_id: String, prompt: String, mode: RoutingMode) -> RequestContext {
    let tokens = tokenize(&prompt);
    let block_hashes = tokens
        .chunks(BLOCK_SIZE)
        .map(hash_block)
        .collect::<Vec<_>>();
    let cumulative_hashes = cumulative_hashes_from_blocks(&block_hashes);

    RequestContext {
        session_id,
        prompt,
        tokens,
        block_hashes,
        cumulative_hashes,
        mode,
    }
}
