const BITS_PER_WORD: usize = 64;

pub type PodId = usize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostBitmap {
    words: Vec<u64>,
}

impl HostBitmap {
    pub const fn empty() -> Self {
        Self { words: Vec::new() }
    }

    pub fn full_for_count(count: usize) -> Self {
        let mut bitmap = Self::with_bit_capacity(count);
        for pod_id in 0..count {
            bitmap.set(pod_id);
        }
        bitmap
    }

    pub fn with_bit_capacity(bits: usize) -> Self {
        Self {
            words: vec![0; words_for_bits(bits)],
        }
    }

    pub fn from_words(words: Vec<u64>) -> Self {
        Self { words }
    }

    pub fn words(&self) -> &[u64] {
        &self.words
    }

    pub fn bit_capacity(&self) -> usize {
        self.words.len() * BITS_PER_WORD
    }

    pub fn highest_set_bit_plus_one(&self) -> usize {
        for (word_index, word) in self.words.iter().enumerate().rev() {
            if *word != 0 {
                let highest_bit = BITS_PER_WORD - 1 - word.leading_zeros() as usize;
                return word_index * BITS_PER_WORD + highest_bit + 1;
            }
        }
        0
    }

    pub fn set(&mut self, pod_id: usize) {
        self.ensure_bit(pod_id);
        self.words[pod_id / BITS_PER_WORD] |= 1_u64 << (pod_id % BITS_PER_WORD);
    }

    pub fn clear(&mut self, pod_id: usize) {
        let Some(word) = self.words.get_mut(pod_id / BITS_PER_WORD) else {
            return;
        };
        *word &= !(1_u64 << (pod_id % BITS_PER_WORD));
    }

    pub fn contains(&self, pod_id: usize) -> bool {
        self.words
            .get(pod_id / BITS_PER_WORD)
            .is_some_and(|word| (word & (1_u64 << (pod_id % BITS_PER_WORD))) != 0)
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

    pub fn count(&self) -> usize {
        self.count_ones()
    }

    pub fn and(&self, other: &Self) -> Self {
        let mut words = vec![0; self.words.len().min(other.words.len())];
        for (index, word) in words.iter_mut().enumerate() {
            *word = self.words[index] & other.words[index];
        }
        Self { words }
    }

    pub fn or(&self, other: &Self) -> Self {
        let mut words = vec![0; self.words.len().max(other.words.len())];
        for (index, word) in words.iter_mut().enumerate() {
            *word = self.words.get(index).copied().unwrap_or(0)
                | other.words.get(index).copied().unwrap_or(0);
        }
        Self { words }
    }

    pub fn minus(&self, other: &Self) -> Self {
        let mut words = vec![0; self.words.len()];
        for (index, word) in words.iter_mut().enumerate() {
            *word = self.words[index] & !other.words.get(index).copied().unwrap_or(0);
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

    fn ensure_bit(&mut self, pod_id: usize) {
        let needed = words_for_bits(pod_id + 1);
        if self.words.len() < needed {
            self.words.resize(needed, 0);
        }
    }
}

impl Default for HostBitmap {
    fn default() -> Self {
        Self::empty()
    }
}

fn words_for_bits(bits: usize) -> usize {
    bits.saturating_add(BITS_PER_WORD - 1) / BITS_PER_WORD
}

#[cfg(test)]
mod tests {
    use super::HostBitmap;

    #[test]
    fn bitmap_intersections_work_across_dynamic_words() {
        let mut a = HostBitmap::empty();
        a.set(0);
        a.set(65);
        a.set(255);

        let mut b = HostBitmap::empty();
        b.set(65);
        b.set(127);

        assert!(a.contains(0));
        assert!(a.contains(65));
        assert_eq!(a.count_ones(), 3);
        assert_eq!(a.and(&b).iter_set_bits(), vec![65]);
        assert_eq!(a.or(&b).iter_set_bits(), vec![0, 65, 127, 255]);
        assert_eq!(a.minus(&b).iter_set_bits(), vec![0, 255]);

        a.clear(65);
        assert_eq!(a.iter_set_bits(), vec![0, 255]);
    }
}
