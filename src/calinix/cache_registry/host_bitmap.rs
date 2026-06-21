pub const MAX_PODS: usize = 256;
const WORDS: usize = 4;
const BITS_PER_WORD: usize = 64;

pub type PodId = usize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostBitmap {
    words: [u64; WORDS],
}

impl HostBitmap {
    pub const fn empty() -> Self {
        Self { words: [0; WORDS] }
    }

    pub fn full_for_count(count: usize) -> Self {
        let mut bitmap = Self::empty();
        let capped = count.min(MAX_PODS);
        let full_words = capped / BITS_PER_WORD;
        for i in 0..full_words {
            bitmap.words[i] = u64::MAX;
        }
        let remaining = capped % BITS_PER_WORD;
        if remaining > 0 && full_words < WORDS {
            bitmap.words[full_words] = (1_u64 << remaining) - 1;
        }
        bitmap
    }

    pub fn words(&self) -> &[u64; WORDS] {
        &self.words
    }

    pub fn highest_set_bit_plus_one(&self) -> usize {
        let mut i = WORDS;
        while i > 0 {
            i -= 1;
            if self.words[i] != 0 {
                let highest_bit = BITS_PER_WORD - 1 - self.words[i].leading_zeros() as usize;
                return i * BITS_PER_WORD + highest_bit + 1;
            }
        }
        0
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
        self.words[0] == 0 && self.words[1] == 0 && self.words[2] == 0 && self.words[3] == 0
    }

    pub fn count_ones(&self) -> usize {
        self.words[0].count_ones() as usize
            + self.words[1].count_ones() as usize
            + self.words[2].count_ones() as usize
            + self.words[3].count_ones() as usize
    }

    pub fn count(&self) -> usize {
        self.count_ones()
    }

    pub fn first_set_bit(&self) -> Option<usize> {
        for (word_index, word) in self.words.iter().enumerate() {
            if *word != 0 {
                return Some(word_index * BITS_PER_WORD + word.trailing_zeros() as usize);
            }
        }
        None
    }

    pub fn and(&self, other: &Self) -> Self {
        Self {
            words: [
                self.words[0] & other.words[0],
                self.words[1] & other.words[1],
                self.words[2] & other.words[2],
                self.words[3] & other.words[3],
            ],
        }
    }

    pub fn or(&self, other: &Self) -> Self {
        Self {
            words: [
                self.words[0] | other.words[0],
                self.words[1] | other.words[1],
                self.words[2] | other.words[2],
                self.words[3] | other.words[3],
            ],
        }
    }

    pub fn minus(&self, other: &Self) -> Self {
        Self {
            words: [
                self.words[0] & !other.words[0],
                self.words[1] & !other.words[1],
                self.words[2] & !other.words[2],
                self.words[3] & !other.words[3],
            ],
        }
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

impl Default for HostBitmap {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::HostBitmap;

    #[test]
    fn bitmap_intersections_work_across_words() {
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

    #[test]
    fn full_for_count_sets_correct_bits() {
        let bm = HostBitmap::full_for_count(3);
        assert_eq!(bm.iter_set_bits(), vec![0, 1, 2]);
        assert_eq!(bm.count_ones(), 3);

        let bm = HostBitmap::full_for_count(64);
        assert_eq!(bm.count_ones(), 64);
        assert!(bm.contains(63));
        assert!(!bm.contains(64));

        let bm = HostBitmap::full_for_count(256);
        assert_eq!(bm.count_ones(), 256);
        assert!(bm.contains(255));
    }

    #[test]
    fn out_of_bounds_pod_is_silently_ignored() {
        let mut bm = HostBitmap::empty();
        bm.set(300);
        assert!(bm.is_empty());
        assert!(!bm.contains(300));
    }

    #[test]
    fn bitmap_is_copy() {
        let a = HostBitmap::full_for_count(8);
        let b = a;
        assert_eq!(a, b);
    }
}
