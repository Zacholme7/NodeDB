use alloy_primitives::{StorageValue, Uint};
use async_trait::async_trait;
use core::future::Future;
use eyre::Result;
use parking_lot::RwLock;
use reth_chainspec::ChainSpecBuilder;
use reth_db::{mdbx::DatabaseArguments, open_db_read_only, ClientVersion, DatabaseEnv};
use reth_node_api::NodeTypesWithDBAdapter;
use reth_node_ethereum::EthereumNode;
use reth_provider::{
    providers::StaticFileProvider, BlockNumReader, ProviderFactory, StateProviderBox,
};
use reth_provider::{ProviderError, StateProvider};
use revm::context::DBErrorMarker;
use revm::database::async_db::DatabaseAsyncRef;
use revm::database::{AccountState, DatabaseAsync};
use revm::primitives::{Address, B256, KECCAK_EMPTY, U256};
use revm::state::{Account, AccountInfo, Bytecode};
use revm::{Database, DatabaseCommit, DatabaseRef};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};

#[derive(Debug)]
pub struct NodeDBError(pub String);

impl fmt::Display for NodeDBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeDBError: {}", self.0)
    }
}

impl Error for NodeDBError {}

impl DBErrorMarker for NodeDBError {}

impl From<ProviderError> for NodeDBError {
    fn from(e: ProviderError) -> Self {
        NodeDBError(e.to_string())
    }
}

/// Synchronous backend trait
pub trait NodeDBBackendSync: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static + DBErrorMarker;

    fn basic_account(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error>;
    fn account_code(&self, address: Address) -> Result<Option<Bytecode>, Self::Error>;
    fn storage(&self, address: Address, slot: U256) -> Result<U256, Self::Error>;
    fn block_hash(&self, number: u64) -> Result<B256, Self::Error>;
}

#[async_trait]
pub trait NodeDBBackendAsync: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static + From<NodeDBError>;

    async fn basic_account(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error>;
    async fn account_code(&self, address: Address) -> Result<Option<Bytecode>, Self::Error>;
    async fn storage(&self, address: Address, slot: U256) -> Result<U256, Self::Error>;
    async fn block_hash(&self, number: u64) -> Result<B256, Self::Error>;
}

/// Main structure for the Node Database
/// It implements the Database trait to provide access to the database
/// and the DatabaseRef trait to provide read-only access.
/// It also implements the DatabaseCommit trait to commit changes to the database.
pub struct NodeDB<B> {
    backend: Arc<B>,
    accounts: HashMap<Address, NodeDBAccount>,
    contracts: HashMap<B256, Bytecode>,
    accessed_slots: RwLock<HashMap<Address, HashSet<U256>>>,
    tracing_enabled: bool,
}

impl<B> NodeDB<B> {
    // Constructor for NodeDB
    pub fn new(backend: B) -> Self {
        Self {
            backend: Arc::new(backend),
            accounts: HashMap::new(),
            contracts: HashMap::new(),
            accessed_slots: RwLock::new(HashMap::new()),
            tracing_enabled: false,
        }
    }

    pub fn enable_tracing(&mut self) -> Result<()> {
        let mut slots = self.accessed_slots.write();

        self.tracing_enabled = true;
        slots.clear();
        Ok(())
    }

    // This is used to stop tracking the accessed slots after a transaction has been executed
    // This is useful to avoid memory leaks and unnecessary
    pub fn disable_tracing(&mut self) {
        self.tracing_enabled = false;
    }

    // Get the accessed slots for a given address
    // This is used for tracing to get the slots that were accessed during a transaction
    pub fn get_accessed_slots(&self, target_address: Address) -> Result<Vec<U256>> {
        let slots = self.accessed_slots.read();
        Ok(slots
            .get(&target_address)
            .map(|slot_set| slot_set.iter().cloned().collect())
            .unwrap_or_default())
    }

    // Insert account information into the database
    pub fn insert_account_info(
        &mut self,
        account_address: Address,
        account_info: AccountInfo,
        insertion_type: InsertionType,
    ) {
        let mut new_account = NodeDBAccount::new(insertion_type);
        new_account.info = account_info;
        self.accounts.insert(account_address, new_account);
    }
}

pub trait NodeDBStorageSync {
    type Error;
    fn insert_account_storage(
        &mut self,
        account_address: Address,
        slot: U256,
        value: U256,
        insertion_type: InsertionType,
    ) -> Result<(), Self::Error>;
}

#[async_trait]
impl<B: NodeDBBackendSync> NodeDBStorageSync for NodeDB<B>
where
    B: NodeDBBackendSync,
    B::Error: From<NodeDBError>,
{
    type Error = B::Error;

    // Insert storage info into the database.
    fn insert_account_storage(
        &mut self,
        account_address: Address,
        slot: U256,
        value: U256,
        insertion_type: InsertionType,
    ) -> Result<(), Self::Error> {
        // If this account already exists, just update the storage slot
        if let Some(account) = self.accounts.get_mut(&account_address) {
            // slot value is marked as custom since this is a custom insertion
            let slot_value = NodeDBSlot {
                value,
                insertion_type: InsertionType::Custom,
            };
            account.storage.insert(slot, slot_value);
            return Ok(());
        }

        // The account does not exist. Fetch account information from provider and insert account
        // into database
        let account = self.backend.basic_account(account_address)?.unwrap();
        self.insert_account_info(account_address, account, insertion_type);

        // The account is now in the database, so fetch and insert the storage value
        let node_db_account = self.accounts.get_mut(&account_address).unwrap();
        let slot_value = NodeDBSlot {
            value,
            insertion_type: InsertionType::Custom,
        };
        node_db_account.storage.insert(slot, slot_value);

        Ok(())
    }
}

#[async_trait]
pub trait NodeDBStorageAsync {
    type Error;
    async fn insert_account_storage(
        &mut self,
        account_address: Address,
        slot: U256,
        value: U256,
        insertion_type: InsertionType,
    ) -> Result<(), Self::Error>;
}

#[async_trait]
impl<B: NodeDBBackendAsync> NodeDBStorageAsync for NodeDB<B>
where
    B: NodeDBBackendAsync,
    B::Error: From<NodeDBError>,
{
    type Error = B::Error;

    async fn insert_account_storage(
        &mut self,
        account_address: Address,
        slot: U256,
        value: U256,
        insertion_type: InsertionType,
    ) -> Result<(), Self::Error> {
        // If this account already exists, just update the storage slot
        if let Some(account) = self.accounts.get_mut(&account_address) {
            // slot value is marked as custom since this is a custom insertion
            let slot_value = NodeDBSlot {
                value,
                insertion_type: InsertionType::Custom,
            };
            account.storage.insert(slot, slot_value);
            return Ok(());
        }

        // The account does not exist. Fetch account information from provider and insert account
        // into database
        let account = self.backend.basic_account(account_address).await?.unwrap();
        self.insert_account_info(account_address, account, insertion_type);

        // The account is now in the database, so fetch and insert the storage value
        let node_db_account = self.accounts.get_mut(&account_address).unwrap();
        let slot_value = NodeDBSlot {
            value,
            insertion_type: InsertionType::Custom,
        };
        node_db_account.storage.insert(slot, slot_value);

        Ok(())
    }
}
/// Implement the DatabaseAsync trait for NodeDB
/// This allows us to use NodeDB as a database
/// It is used internally by the NodeDB to fetch data from the chain
/// Note: It is not used directly by the user
impl<B: NodeDBBackendAsync> DatabaseAsync for NodeDB<B>
where
    B::Error: DBErrorMarker + From<NodeDBError>,
{
    type Error = B::Error;

    fn basic_async(
        &mut self,
        address: Address,
    ) -> impl Future<Output = Result<Option<AccountInfo>, Self::Error>> + Send {
        async move {
            // If the account exists and it is custom, we know there is no corresponding on chain state
            if let Some(account) = self.accounts.get(&address) {
                if account.insertion_type == InsertionType::Custom {
                    return Ok(Some(account.info.clone()));
                }
            }

            // Fetch the account from the chain
            let account_info_opt = self.basic_async_ref(address).await?;

            let account_info = match account_info_opt {
                Some(info) => info,
                None => return Ok(None),
            };

            match self.accounts.get_mut(&address) {
                Some(account) => account.info = account_info.clone(),
                None => {
                    self.insert_account_info(address, account_info.clone(), InsertionType::OnChain)
                }
            }

            Ok(Some(account_info))
        }
    }

    fn code_by_hash_async(
        &mut self,
        _code_hash: B256,
    ) -> impl Future<Output = Result<Bytecode, Self::Error>> + Send {
        async move {
            Err(Self::Error::from(NodeDBError(
                "This should not be called, as the code is already loaded".to_string(),
            )))
        }
    }

    fn storage_async(
        &mut self,
        address: Address,
        index: U256,
    ) -> impl Future<Output = Result<U256, Self::Error>> + Send {
        async move {
            // Log slot access if tracing is enabled
            if self.tracing_enabled {
                let mut slots = self.accessed_slots.write();
                slots.entry(address).or_default().insert(index);
            }

            // Check if the account and the slot exist
            if let Some(account) = self.accounts.get(&address) {
                if let Some(value) = account.storage.get(&index) {
                    // The slot is in storage. If it is custom, there is no corresponding onchain state
                    // to update it with, just return the value
                    if value.insertion_type == InsertionType::Custom {
                        return Ok(value.value);
                    }
                    // The account exists and the slot is onchain, continue on so it is fetched and updated
                }
            }

            // Fetch the storage value
            let value = self.backend.storage(address, index).await?;

            // If the account exists, just update the storage. Otherwise, fetch and create a new
            // account before inserting the storage value
            match self.accounts.get_mut(&address) {
                Some(account) => {
                    account.storage.insert(
                        index,
                        NodeDBSlot {
                            value,
                            insertion_type: InsertionType::OnChain,
                        },
                    );
                }
                None => {
                    let _ = Self::basic_async(self, address).await?;
                    let account = self.accounts.get_mut(&address).unwrap();
                    account.storage.insert(
                        index,
                        NodeDBSlot {
                            value,
                            insertion_type: InsertionType::OnChain,
                        },
                    );
                }
            }
            Ok(value)
        }
    }

    fn block_hash_async(
        &mut self,
        number: u64,
    ) -> impl Future<Output = Result<B256, <B as NodeDBBackendAsync>::Error>> + Send {
        Self::block_hash_async_ref(self, number)
    }
}

impl<B> DatabaseAsync for &mut NodeDB<B>
where
    B: NodeDBBackendAsync,
    B::Error: DBErrorMarker + std::error::Error,
{
    type Error = B::Error;

    fn basic_async(
        &mut self,
        address: Address,
    ) -> impl Future<Output = Result<Option<AccountInfo>, Self::Error>> + Send {
        <NodeDB<B> as DatabaseAsync>::basic_async(self, address)
    }

    fn code_by_hash_async(
        &mut self,
        code_hash: B256,
    ) -> impl Future<Output = Result<Bytecode, Self::Error>> + Send {
        <NodeDB<B> as DatabaseAsync>::code_by_hash_async(self, code_hash)
    }

    fn storage_async(
        &mut self,
        address: Address,
        index: U256,
    ) -> impl Future<Output = Result<StorageValue, Self::Error>> + Send {
        <NodeDB<B> as DatabaseAsync>::storage_async(self, address, index)
    }

    fn block_hash_async(
        &mut self,
        number: u64,
    ) -> impl Future<Output = Result<B256, Self::Error>> + Send {
        <NodeDB<B> as DatabaseAsync>::block_hash_async(self, number)
    }
}

impl<B: NodeDBBackendSync> Database for NodeDB<B>
where
    B: NodeDBBackendSync,
    B::Error: From<NodeDBError>,
{
    type Error = B::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // If the account exists and it is custom, we know there is no corresponding on chain state
        if let Some(account) = self.accounts.get(&address) {
            if account.insertion_type == InsertionType::Custom {
                return Ok(Some(account.info.clone()));
            }
        }

        // Fetch the account from the chain
        let account_info_opt = self.basic_ref(address)?;

        let account_info = match account_info_opt {
            Some(info) => info,
            None => return Ok(None),
        };

        match self.accounts.get_mut(&address) {
            Some(account) => account.info = account_info.clone(),
            None => self.insert_account_info(address, account_info.clone(), InsertionType::OnChain),
        }

        Ok(Some(account_info))
    }

    fn code_by_hash(&mut self, _hash: B256) -> Result<Bytecode, Self::Error> {
        Err(NodeDBError("This should not be called, as the code is already loaded".into()).into())
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        // Log slot access if tracing is enabled
        if self.tracing_enabled {
            let mut slots = self.accessed_slots.write();
            slots.entry(address).or_default().insert(index);
        }

        // Check if the account and the slot exist
        if let Some(account) = self.accounts.get(&address) {
            if let Some(value) = account.storage.get(&index) {
                // The slot is in storage. If it is custom, there is no corresponding onchain state
                // to update it with, just return the value
                if value.insertion_type == InsertionType::Custom {
                    return Ok(value.value);
                }
                // The account exists and the slot is onchain, continue on so it is fetched and updated
            }
        }

        // Fetch the storage value
        let value = self.backend.storage(address, index)?;

        // If the account exists, just update the storage. Otherwise, fetch and create a new
        // account before inserting the storage value
        match self.accounts.get_mut(&address) {
            Some(account) => {
                account.storage.insert(
                    index,
                    NodeDBSlot {
                        value,
                        insertion_type: InsertionType::OnChain,
                    },
                );
            }
            None => {
                let _ = Self::basic(self, address)?;
                let account = self.accounts.get_mut(&address).unwrap();
                account.storage.insert(
                    index,
                    NodeDBSlot {
                        value,
                        insertion_type: InsertionType::OnChain,
                    },
                );
            }
        }
        Ok(value)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.block_hash_ref(number)
    }
}

impl<B: NodeDBBackendSync> DatabaseRef for NodeDB<B>
where
    B: NodeDBBackendSync,
    B::Error: From<NodeDBError>,
{
    type Error = B::Error;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // If the account exists and it is custom, just return it. Otherwise update from the chain
        if let Some(account) = self.accounts.get(&address) {
            if account.insertion_type == InsertionType::Custom {
                return Ok(Some(account.info.clone()));
            }
        }

        let account_opt = self.backend.basic_account(address)?;

        let account = match account_opt {
            Some(acc) => acc,
            None => return Ok(None),
        };

        let code = self.backend.account_code(address)?.unwrap_or_default();

        let info = AccountInfo::new(account.balance, account.nonce, code.hash_slow(), code);

        Ok(Some(info))
    }

    fn code_by_hash_ref(&self, _hash: B256) -> Result<Bytecode, Self::Error> {
        Err(NodeDBError("This should not be called, as the code is already loaded".into()).into())
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        if self.tracing_enabled {
            let mut slots = self.accessed_slots.write();
            slots.entry(address).or_default().insert(index);
        }

        // Check if the account and the slot exist locally with custom insertion
        if let Some(account) = self.accounts.get(&address) {
            if let Some(value) = account.storage.get(&index) {
                if value.insertion_type == InsertionType::Custom {
                    return Ok(value.value);
                }
            }
        }
        let value = self.backend.storage(address, index)?;
        Ok(value)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.backend.block_hash(number)
    }
}

/// Implement the DatabaseAsyncRef trait for NodeDB
/// This allows us to use NodeDB as a read-only database
/// It is used internally by the NodeDB to fetch data from the chain
/// Note: It is not used directly by the user
impl<B: NodeDBBackendAsync> DatabaseAsyncRef for NodeDB<B>
where
    B::Error: DBErrorMarker + From<NodeDBError>,
{
    type Error = B::Error;

    fn basic_async_ref(
        &self,
        address: Address,
    ) -> impl Future<Output = Result<Option<AccountInfo>, Self::Error>> + Send {
        async move {
            // If the account exists and it is custom, just return it. Otherwise update from the chain
            if let Some(account) = self.accounts.get(&address) {
                if account.insertion_type == InsertionType::Custom {
                    return Ok(Some(account.info.clone()));
                }
            }

            let account_opt = self.backend.basic_account(address).await?;

            let account = match account_opt {
                Some(acc) => acc,
                None => return Ok(None),
            };

            let code = self
                .backend
                .account_code(address)
                .await?
                .unwrap_or_default();

            let info = AccountInfo::new(account.balance, account.nonce, code.hash_slow(), code);

            Ok(Some(info))
        }
    }

    fn code_by_hash_async_ref(
        &self,
        _code_hash: B256,
    ) -> impl Future<Output = Result<Bytecode, Self::Error>> + Send {
        async move {
            Err(Self::Error::from(NodeDBError(
                "This should not be called, as the code is already loaded".to_string(),
            )))
        }
    }

    fn storage_async_ref(
        &self,
        address: Address,
        index: U256,
    ) -> impl Future<Output = Result<U256, Self::Error>> + Send {
        // Log slot access if tracing is enabled
        async move {
            if self.tracing_enabled {
                let mut slots = self.accessed_slots.write();
                slots.entry(address).or_default().insert(index);
            }

            // Check if the account and the slot exist locally with custom insertion
            if let Some(account) = self.accounts.get(&address) {
                if let Some(value) = account.storage.get(&index) {
                    if value.insertion_type == InsertionType::Custom {
                        return Ok(value.value);
                    }
                }
            }
            let value = self.backend.storage(address, index).await?;
            Ok(value)
        }
    }

    fn block_hash_async_ref(
        &self,
        number: u64,
    ) -> impl Future<Output = Result<B256, <B as NodeDBBackendAsync>::Error>> + Send {
        async move {
            let hash = self.backend.block_hash(number).await?;
            Ok(hash)
        }
    }
}

impl<B> DatabaseAsyncRef for &mut NodeDB<B>
where
    B: NodeDBBackendAsync,
    B::Error: DBErrorMarker + std::error::Error,
{
    type Error = B::Error;

    fn basic_async_ref(
        &self,
        address: Address,
    ) -> impl Future<Output = Result<Option<AccountInfo>, Self::Error>> + Send {
        <NodeDB<B> as DatabaseAsyncRef>::basic_async_ref(self, address)
    }

    fn code_by_hash_async_ref(
        &self,
        code_hash: B256,
    ) -> impl Future<Output = Result<Bytecode, Self::Error>> + Send {
        <NodeDB<B> as DatabaseAsyncRef>::code_by_hash_async_ref(self, code_hash)
    }

    fn storage_async_ref(
        &self,
        address: Address,
        index: U256,
    ) -> impl Future<Output = Result<StorageValue, Self::Error>> + Send {
        <NodeDB<B> as DatabaseAsyncRef>::storage_async_ref(self, address, index)
    }

    fn block_hash_async_ref(
        &self,
        number: u64,
    ) -> impl Future<Output = Result<B256, Self::Error>> + Send {
        <NodeDB<B> as DatabaseAsyncRef>::block_hash_async_ref(self, number)
    }
}

/// Implement the DatabaseCommit trait for NodeDB
/// It is used internally by the NodeDB to commit changes to the database
/// Note: It is not used directly by the user
impl<B: NodeDBBackendAsync> DatabaseCommit for NodeDB<B> {
    fn commit(&mut self, changes: HashMap<Address, Account, foldhash::fast::RandomState>) {
        for (address, mut account) in changes {
            if !account.is_touched() {
                continue;
            }
            if account.is_selfdestructed() {
                let db_account = self.accounts.entry(address).or_default();
                db_account.storage.clear();
                db_account.state = AccountState::NotExisting;
                db_account.info = AccountInfo::default();
                continue;
            }
            let is_newly_created = account.is_created();

            if let Some(code) = &mut account.info.code {
                if !code.is_empty() {
                    if account.info.code_hash == KECCAK_EMPTY {
                        account.info.code_hash = code.hash_slow();
                    }
                    self.contracts
                        .entry(account.info.code_hash)
                        .or_insert_with(|| code.clone());
                }
            }

            let db_account = self.accounts.entry(address).or_default();
            db_account.info = account.info;

            db_account.state = if is_newly_created {
                db_account.storage.clear();
                AccountState::StorageCleared
            } else if db_account.state.is_storage_cleared() {
                // Preserve old account state if it already exists
                AccountState::StorageCleared
            } else {
                AccountState::Touched
            };
            db_account
                .storage
                .extend(account.storage.into_iter().map(|(key, value)| {
                    (
                        key,
                        NodeDBSlot {
                            value: value.present_value(),
                            insertion_type: InsertionType::Custom,
                        },
                    )
                }));
        }
    }
}

// Enum representing if an account was fetched from the chain or
// custom data that was inserted into the database
#[derive(Default, Eq, PartialEq, Copy, Clone, Debug)]
pub enum InsertionType {
    Custom,
    #[default]
    OnChain,
}

// Struct representing a storage slot in the database.
// If we insert custom storage, we do not want to fetch
// from the chain and overwrite the data. By signaling each slot
// as custom or onchain, we can accomplish this
#[derive(Default, Eq, PartialEq, Copy, Clone, Debug)]
pub struct NodeDBSlot {
    value: U256,
    insertion_type: InsertionType,
}

// Structure to represent an account in the NodeDB
#[derive(Default, Clone, Debug)]
struct NodeDBAccount {
    pub info: AccountInfo,
    pub state: AccountState,
    pub storage: HashMap<U256, NodeDBSlot>,
    pub insertion_type: InsertionType,
}

impl NodeDBAccount {
    pub fn new(insertion_type: InsertionType) -> Self {
        Self {
            info: AccountInfo::default(),
            state: AccountState::NotExisting,
            storage: HashMap::new(),
            insertion_type,
        }
    }
}

/// A backend for read-only access to a Reth database.
///
/// Internally it holds:
/// - a [`ProviderFactory`] to build state providers,  
/// - a `RwLock<StateProviderBox>` for thread-safe reads,  
/// - an `AtomicU64` tracking the last synced block.
///
/// Designed for use in multithreaded contexts alongside [`NodeDB`].
pub struct RethBackend {
    factory: ProviderFactory<NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>>,
    provider: RwLock<StateProviderBox>,
    db_block: AtomicU64,
}

impl RethBackend {
    /// Opens the database in read-only mode and builds the initial provider.
    /// # Arguments
    /// * `db_path` – directory containing the `db/` subfolder and optional `static_files/`.
    /// # Errors
    /// Returns a [`NodeDBError`] if opening the DB or initializing the provider fails.
    /// # Examples
    /// ```no_run
    /// # use std::path::Path;
    /// # use reth_backend::RethBackend;
    /// let backend = RethBackend::new(Path::new("/path/to/db"))
    ///     .expect("failed to open Reth database");
    /// ```
    pub fn new(db_path: &Path) -> Result<Self> {
        let db = Arc::new(open_db_read_only(
            db_path.join("db").as_path(),
            DatabaseArguments::new(ClientVersion::default()),
        )?);
        let spec = Arc::new(ChainSpecBuilder::mainnet().build());
        let factory = ProviderFactory::new(
            db,
            spec,
            StaticFileProvider::read_only(db_path.join("static_files"), true)?,
        );
        let provider = factory.latest()?;
        let db_block = factory.last_block_number()?;

        Ok(Self {
            factory,
            provider: RwLock::new(provider),
            db_block: AtomicU64::new(db_block),
        })
    }

    /// Checks the latest block number and refreshes the provider if it has advanced.
    /// This avoids unnecessary rebuilds when no new blocks have been produced.
    /// # Errors
    /// Returns a [`NodeDBError`] if querying the block number or fetching the new
    /// provider fails.
    fn update_provider(&self) -> Result<()> {
        if let Ok(current_block) = self.factory.last_block_number() {
            if current_block > self.db_block.load(Ordering::Relaxed) {
                self.db_block.store(current_block, Ordering::Relaxed);
                *self.provider.write() = self.factory.latest()?;
            }
        }
        Ok(())
    }
}

/// Implements the [`NodeDBBackendAsync`] trait for the `RethBackend`.
/// This allows the `RethBackend` to be used as a backend for the `NodeDB`.
/// The `NodeDBBackendAsync` trait is used to abstract the database access layer.
#[async_trait]
impl NodeDBBackendAsync for RethBackend {
    type Error = NodeDBError;

    /// Get basic account information.
    ///
    /// Returns `None` if the account doesn't exist.
    async fn basic_account(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.update_provider()
            .map_err(|e| NodeDBError(e.to_string()))?;

        let provider = self.provider.read();
        let account = provider.basic_account(&address)?;
        let code = provider.account_code(&address)?;

        let account_info = match account {
            Some(acc) => AccountInfo::new(
                acc.balance,
                acc.nonce,
                code.as_ref().map_or(KECCAK_EMPTY, |c| c.hash_slow()),
                code.map_or(Bytecode::new(), |c| Bytecode::new_raw(c.original_bytes())),
            ),
            None => return Ok(None),
        };

        Ok(Some(account_info))
    }

    /// Get storage of given account.
    async fn storage(&self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        self.update_provider()
            .map_err(|e| NodeDBError(e.to_string()))?;

        let provider = self.provider.read();
        let value = provider
            .storage(address, slot.into())?
            .unwrap_or(U256::ZERO);
        Ok(value)
    }

    /// Get the hash of the block with the given number. Returns `None` if no block with this number
    /// exists.
    async fn block_hash(&self, number: u64) -> Result<B256, Self::Error> {
        self.update_provider()
            .map_err(|e| NodeDBError(e.to_string()))?;

        let provider = self.provider.read();
        let hash = provider.block_hash(number)?.unwrap_or(KECCAK_EMPTY);
        Ok(B256::new(hash.0))
    }

    /// Get the code of the account at the given address.
    /// Returns `None` if the account doesn't exist or has no code.
    async fn account_code(&self, address: Address) -> Result<Option<Bytecode>, Self::Error> {
        self.update_provider()
            .map_err(|e| NodeDBError(e.to_string()))?;
        let provider = self.provider.read();
        let account_code = provider.account_code(&address)?;
        Ok(account_code.map(|code| Bytecode::new_raw(code.original_bytes())))
    }
}

impl NodeDBBackendSync for RethBackend {
    type Error = NodeDBError;

    fn basic_account(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.update_provider()
            .map_err(|e| NodeDBError(e.to_string()))?;

        let prov = self.provider.read();
        let acct_opt = prov.basic_account(&address)?;
        let code_opt = prov.account_code(&address)?;

        let acct_info = match acct_opt {
            Some(acc) => {
                let hash = code_opt.as_ref().map_or(KECCAK_EMPTY, |c| c.hash_slow());
                let code = code_opt
                    .map(|c| Bytecode::new_raw(c.original_bytes()))
                    .unwrap_or_default();
                AccountInfo::new(acc.balance, acc.nonce, hash, code)
            }
            None => return Ok(None),
        };
        Ok(Some(acct_info))
    }

    fn account_code(&self, address: Address) -> Result<Option<Bytecode>, Self::Error> {
        self.update_provider()
            .map_err(|e| NodeDBError(e.to_string()))?;
        let prov = self.provider.read();
        let code_opt = prov.account_code(&address)?;
        Ok(code_opt.map(|c| Bytecode::new_raw(c.original_bytes())))
    }

    fn storage(&self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        self.update_provider()
            .map_err(|e| NodeDBError(e.to_string()))?;
        let prov = self.provider.read();
        Ok(prov.storage(address, slot.into())?.unwrap_or(U256::ZERO))
    }

    fn block_hash(&self, number: u64) -> Result<B256, Self::Error> {
        self.update_provider()
            .map_err(|e| NodeDBError(e.to_string()))?;
        let prov = self.provider.read();
        let raw = prov.block_hash(number)?.unwrap_or(KECCAK_EMPTY);
        Ok(B256::new(raw.0))
    }
}

#[derive(Debug)]
pub struct NodeDBAsync<T> {
    inner: T,
    rt: HandleOrRuntime,
}

#[derive(Debug)]
enum HandleOrRuntime {
    Handle(Handle),
    Runtime(Runtime),
}

impl HandleOrRuntime {
    fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        match self {
            HandleOrRuntime::Handle(h) => tokio::task::block_in_place(|| h.block_on(fut)),
            HandleOrRuntime::Runtime(rt) => rt.block_on(fut),
        }
    }
}

impl<T> NodeDBAsync<T>
where
    T: DatabaseAsync + DatabaseAsyncRef,
{
    pub fn new(inner: T) -> Option<Self> {
        let rt = Handle::try_current().ok().and_then(|h| {
            if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                None
            } else {
                Some(HandleOrRuntime::Handle(h))
            }
        })?;
        Some(Self { inner, rt })
    }

    pub fn with_runtime(inner: T, rt: Runtime) -> Self {
        Self {
            inner,
            rt: HandleOrRuntime::Runtime(rt),
        }
    }

    pub fn with_handle(inner: T, h: Handle) -> Self {
        Self {
            inner,
            rt: HandleOrRuntime::Handle(h),
        }
    }
}

impl<T> Database for NodeDBAsync<T>
where
    T: DatabaseAsync,
{
    type Error = T::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.rt.block_on(self.inner.basic_async(address))
    }
    fn code_by_hash(&mut self, hash: B256) -> Result<Bytecode, Self::Error> {
        self.rt.block_on(self.inner.code_by_hash_async(hash))
    }
    fn storage(&mut self, addr: Address, idx: Uint<256, 4>) -> Result<StorageValue, Self::Error> {
        self.rt.block_on(self.inner.storage_async(addr, idx))
    }
    fn block_hash(&mut self, n: u64) -> Result<B256, Self::Error> {
        self.rt.block_on(self.inner.block_hash_async(n))
    }
}

impl<T> DatabaseRef for NodeDBAsync<T>
where
    T: DatabaseAsyncRef,
{
    type Error = T::Error;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.rt.block_on(self.inner.basic_async_ref(address))
    }
    fn code_by_hash_ref(&self, hash: B256) -> Result<Bytecode, Self::Error> {
        self.rt.block_on(self.inner.code_by_hash_async_ref(hash))
    }
    fn storage_ref(&self, addr: Address, idx: Uint<256, 4>) -> Result<StorageValue, Self::Error> {
        self.rt.block_on(self.inner.storage_async_ref(addr, idx))
    }
    fn block_hash_ref(&self, n: u64) -> Result<B256, Self::Error> {
        self.rt.block_on(self.inner.block_hash_async_ref(n))
    }
}

impl<T> DatabaseCommit for NodeDBAsync<T>
where
    T: DatabaseCommit,
{
    fn commit(&mut self, changes: HashMap<Address, Account, foldhash::fast::RandomState>) {
        self.inner.commit(changes)
    }
}
