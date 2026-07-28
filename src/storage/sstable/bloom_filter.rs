use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(super) trait BloomFilter {
    fn insert(&mut self, item: &[u8]);
    fn contains(&self, item: &[u8]) -> bool;
}

#[derive(Serialize, Deserialize)]
pub(super) struct SimpleBloomFilter {
    chunks: Vec<u64>,
    num_chunks: u64,
}

impl SimpleBloomFilter {
    pub(super) fn new(num_chunks: u64) -> Self {
        Self {
            chunks: vec![0; num_chunks as usize],
            num_chunks,
        }
    }

    fn hash_item(&self, item: &[u8]) -> (u64, u64) {
        let mut hasher = Sha256::new();
        hasher.update(item);
        let hash = hasher.finalize();

        // Use the first 16 bytes of the hash to create two u64 values
        let hash1 = u64::from_be_bytes(<[u8; 8]>::try_from(&hash[..8]).unwrap());
        let hash2 = u64::from_be_bytes(<[u8; 8]>::try_from(&hash[8..16]).unwrap());
        (hash1, hash2)
    }

    fn bit_positions(&self, item: &[u8]) -> [(usize, u64); 7] {
        let (hash1, hash2) = self.hash_item(item);
        let total_bits = self.num_chunks * 64;
        std::array::from_fn(|i| {
            let k = (i + 1) as u64;
            let bit_in_spectrum = hash1.wrapping_add(k.wrapping_mul(hash2)) % total_bits;
            let chunk_index = (bit_in_spectrum / 64) as usize;
            let mask = 1u64 << (bit_in_spectrum % 64);
            (chunk_index, mask)
        })
    }
}

impl BloomFilter for SimpleBloomFilter {
    fn insert(&mut self, item: &[u8]) {
        for (chunk_index, mask) in self.bit_positions(item) {
            self.chunks[chunk_index] |= mask;
        }
    }

    fn contains(&self, item: &[u8]) -> bool {
        self.bit_positions(item)
            .iter()
            .all(|&(chunk_index, mask)| self.chunks[chunk_index] & mask != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_false_negatives() {
        let mut bf = SimpleBloomFilter::new(150);
        let items: Vec<Vec<u8>> = (0..1000u32).map(|i| i.to_be_bytes().to_vec()).collect();
        for item in &items {
            bf.insert(item);
        }
        for item in &items {
            assert!(
                bf.contains(item),
                "bloom filter must never produce false negatives"
            );
        }
    }

    #[test]
    fn test_empty_filter_contains_nothing() {
        let bf = SimpleBloomFilter::new(150);
        let mut found = 0;
        for i in 0..10_000u32 {
            if bf.contains(&i.to_be_bytes()) {
                found += 1;
            }
        }
        assert_eq!(found, 0, "empty filter should have zero positives");
    }

    #[test]
    fn test_contains_returns_false_for_not_inserted_item() {
        let mut bf = SimpleBloomFilter::new(150);
        bf.insert(b"hello");
        assert!(!bf.contains(b"world"));
    }

    #[test]
    fn test_insert_is_idempotent() {
        let mut bf = SimpleBloomFilter::new(10);
        bf.insert(b"key");
        let chunks_after_first = bf.chunks.clone();
        bf.insert(b"key");
        assert_eq!(bf.chunks, chunks_after_first);
    }

    #[test]
    fn test_insert_sets_bits() {
        let mut bf = SimpleBloomFilter::new(10);
        assert!(bf.chunks.iter().all(|&c| c == 0));
        bf.insert(b"test");
        assert!(bf.chunks.iter().any(|&c| c != 0));
    }

    #[test]
    fn test_different_keys_set_different_bits() {
        let mut bf1 = SimpleBloomFilter::new(10);
        bf1.insert(b"key_a");

        let mut bf2 = SimpleBloomFilter::new(10);
        bf2.insert(b"key_b");

        assert_ne!(bf1.chunks, bf2.chunks);
    }

    #[test]
    fn test_single_chunk_filter() {
        let mut bf = SimpleBloomFilter::new(1);
        bf.insert(b"a");
        assert!(bf.contains(b"a"));
    }

    #[test]
    fn test_bit_positions_deterministic() {
        let bf = SimpleBloomFilter::new(10);
        let pos1 = bf.bit_positions(b"test");
        let pos2 = bf.bit_positions(b"test");
        assert_eq!(pos1, pos2);
    }

    #[test]
    fn test_bit_positions_within_bounds() {
        let bf = SimpleBloomFilter::new(10);
        for i in 0..100u32 {
            let positions = bf.bit_positions(&i.to_be_bytes());
            for (chunk_index, _) in positions {
                assert!(chunk_index < 10);
            }
        }
    }
}
