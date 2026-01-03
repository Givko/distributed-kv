use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::Hasher,
};

fn main() {
    let keys = gen_keys(100000);
    let ring = create_initial_hash_ring(vec!["node0", "node1", "node2"]);
    eprintln!("Ring size: {}", ring.len()); // ← ADD THIS LINE
    let mut map = HashMap::new();
    let mut key_map = HashMap::new();
    for k in keys {
        let n = choose_node(&k, &ring);
        map.entry(n.clone()).and_modify(|e| *e += 1).or_insert(1);
        key_map.insert(k, n);
    }

    eprintln!("First partitioning");
    eprintln!("========================");
    for (k, v) in map.iter() {
        eprintln!("node {k}: {v} keys");
    }

    let keys_2 = gen_keys(100000);
    let mut map_2 = HashMap::new();
    let ring2 = create_initial_hash_ring(vec!["node0", "node1", "node2", "node3"]);
    let mut key_map_2 = HashMap::new();
    for k in keys_2 {
        let n = choose_node(&k, &ring2);
        map_2.entry(n.clone()).and_modify(|e| *e += 1).or_insert(1);
        key_map_2.insert(k, n);
    }

    eprintln!("========================");
    eprintln!("Seconds partitioning");
    eprintln!("========================");
    for (k, v) in map_2.iter() {
        eprintln!("node {k}: {v} keys");
    }

    eprintln!("========================");
    let mut keys_moved = 0;
    for k in key_map_2.keys() {
        let n1 = key_map.get(k).unwrap();
        let n2 = key_map_2.get(k).unwrap();
        if n1 != n2 {
            keys_moved += 1;
        }
    }
    eprintln!("Keys moved: {keys_moved}");
}

fn gen_keys(num: usize) -> Vec<String> {
    let mut keys = Vec::new();
    for i in 0..num {
        keys.push(format!("user_{i}"));
    }

    keys
}

fn range_partitioning(key: &str, ranges: &[(usize, usize, usize)]) -> usize {
    let key_num: usize = key.strip_prefix("user_").unwrap().parse().unwrap();

    for (start, end, node) in ranges {
        if key_num >= *start && key_num <= *end {
            return *node;
        }
    }

    // in our learning project we will never hit this but the compiler
    // needs it to be satisfied
    panic!();
}

fn hash_partition(key: &str, num_nodes: usize) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(key.as_bytes());
    let hash_value = hasher.finish();
    let node: u64 = hash_value % num_nodes as u64;
    node
}

fn create_initial_hash_ring(nodes: Vec<&str>) -> Vec<(u64, &str)> {
    let mut hash_ring = Vec::new();
    for n in nodes {
        for num in 0..256 {
            let vnode_key = format!("{n}_{num}");
            let mut hasher = DefaultHasher::new();
            hasher.write(vnode_key.as_bytes());
            let hash_value = hasher.finish();
            hash_ring.push((hash_value, n));
        }
    }

    hash_ring.sort_by_key(|(pos, _)| *pos);
    hash_ring
}

fn choose_node(key: &str, hash_ring: &Vec<(u64, &str)>) -> String {
    let mut hasher = DefaultHasher::new();
    hasher.write(key.as_bytes());
    let hash_value = hasher.finish();

    let node_index = hash_ring.binary_search_by(|(pos, _)| pos.cmp(&hash_value));
    let (_, node) = match node_index {
        Ok(index) => hash_ring.get(index).unwrap(),
        Err(index) => {
            if index >= hash_ring.len() {
                hash_ring.first().unwrap()
            } else {
                hash_ring.get(index).unwrap()
            }
        }
    };

    (*node).to_owned()
}
