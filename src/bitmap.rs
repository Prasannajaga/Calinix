#![allow(dead_code)]

pub const MAX_PODS: usize = 256;
const WORDS: usize = 4;
const BITS_PER_WORD: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostBitmap {
    words: [u64; WORDS],
}

impl HostBitmap {
    pub fn empty() -> Self {
        Self { words: [0; WORDS] }
    }

    pub fn full_for_count(count: usize) -> Self {
        let mut bitmap = Self::empty();
        for pod_id in 0..count.min(MAX_PODS) {
            bitmap.set(pod_id);
        }
        bitmap
    }

    pub fn set(&mut self, pod_id: usize) {
        if pod_id >= MAX_PODS {
            return;
        }
        self.words[pod_id / BITS_PER_WORD] |= 1_u64 << (pod_id % BITS_PER_WORD);
    }

    pub fn clear(&mut self, pod_id: usize) {
        if pod_id >= MAX_PODS {
            return;
        }
        self.words[pod_id / BITS_PER_WORD] &= !(1_u64 << (pod_id % BITS_PER_WORD));
    }

    pub fn contains(&self, pod_id: usize) -> bool {
        if pod_id >= MAX_PODS {
            return false;
        }
        (self.words[pod_id / BITS_PER_WORD] & (1_u64 << (pod_id % BITS_PER_WORD))) != 0
    }

    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|word| *word == 0)
    }

    pub fn count_ones(&self) -> usize {
        self.words
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }

    pub fn and(&self, other: Self) -> Self {
        let mut words = [0; WORDS];
        for (i, word) in words.iter_mut().enumerate() {
            *word = self.words[i] & other.words[i];
        }
        Self { words }
    }

    pub fn or(&self, other: Self) -> Self {
        let mut words = [0; WORDS];
        for (i, word) in words.iter_mut().enumerate() {
            *word = self.words[i] | other.words[i];
        }
        Self { words }
    }

    pub fn minus(&self, other: Self) -> Self {
        let mut words = [0; WORDS];
        for (i, word) in words.iter_mut().enumerate() {
            *word = self.words[i] & !other.words[i];
        }
        Self { words }
    }

    pub fn iter_set_bits(&self) -> Vec<usize> {
        let mut bits = Vec::new();
        self.for_each_set_bit(|pod_id| bits.push(pod_id));
        bits
    }

    pub fn for_each_set_bit(&self, mut visit: impl FnMut(usize)) {
        for (word_index, word) in self.words.iter().enumerate() {
            let mut remaining = *word;
            while remaining != 0 {
                let bit = remaining.trailing_zeros() as usize;
                visit(word_index * BITS_PER_WORD + bit);
                remaining &= remaining - 1;
            }
        }
    }
}
