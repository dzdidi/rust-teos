use std::path::PathBuf;
use std::sync::Arc;

pub mod persister;

pub use crate::storage::persister::{Persister, PersisterError};

mod dbm;
use dbm::DBM;

mod encryption;
pub(crate) mod kv;
mod namespace;
use kv::{DynStore, KVStorage};

#[cfg(test)]
pub mod mock_kv;

#[cfg(test)]
pub use mock_kv::MemoryStore;

pub fn create_storage(
    config: StorageConfig,
) -> Result<Box<dyn persister::Persister>, PersisterError> {
    match config {
        StorageConfig::SQL { db_path } => match DBM::new(&db_path) {
            Ok(storage) => Ok(Box::new(storage)),
            Err(e) => Err(PersisterError::Other(format!(
                "Error creating storage: {}",
                e
            ))),
        },
        StorageConfig::KV { kv_store, sk } => match KVStorage::new(kv_store, sk) {
            Ok(storage) => Ok(Box::new(storage)),
            Err(e) => Err(PersisterError::Other(format!(
                "Error creating storage: {}",
                e
            ))),
        },
    }
}

pub enum StorageConfig {
    SQL {
        db_path: PathBuf,
    },
    KV {
        kv_store: Arc<DynStore>,
        sk: Vec<u8>,
    },
}
