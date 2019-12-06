use std::collections::HashMap;

#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            store: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, val: String) {
        self.store.insert(key, val);
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.store.get(&key).cloned()
    }

    pub fn remove(&mut self, key: String) -> Option<String> {
        self.store.remove(&key)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.store.contains_key(key)
    }
}
