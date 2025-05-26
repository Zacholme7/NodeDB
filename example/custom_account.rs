use std::path::Path;

use alloy_primitives::{address, U256};
use alloy_sol_types::{sol, SolCall, SolValue};
use eyre::anyhow;
use eyre::Result;
use node_db::RethBackend;
use node_db::{InsertionType, NodeDB};
use revm::context::result::ExecutionResult;
use revm::primitives::TxKind;
use revm::state::{AccountInfo, Bytecode};
use revm::{Context, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext};

// Generate contract bindings
sol!(Counter, "example/counter.json");

fn main() -> Result<()> {
    // dummy addresses
    let counter_address = address!("A5C381211A406b48A073E954e6949B0D49506bc0");
    let caller = address!("0000000000000000000000000000000000000001");

    // construct the database
    let database_path: String = std::env::var("DB_PATH").unwrap();
    let backend =
        RethBackend::new(Path::new(database_path.as_str())).expect("failed to open Reth database");
    let mut nodedb = NodeDB::new(backend);

    // insert contract account
    let counter_bytecode = Bytecode::new_raw(Counter::DEPLOYED_BYTECODE.clone());
    let counter_bytecode_hash = counter_bytecode.hash_slow();
    let counter_account = AccountInfo {
        balance: U256::ZERO,
        nonce: 0_u64,
        code: Some(counter_bytecode),
        code_hash: counter_bytecode_hash,
    };
    nodedb.insert_account_info(counter_address, counter_account, InsertionType::Custom);

    let increment_calldata = Counter::incrementCall {}.abi_encode();

    // construct the evm instance
    let mut evm = Context::mainnet()
        .with_db(&mut nodedb)
        .modify_tx_chained(|tx| {
            tx.caller = caller;
            tx.kind = TxKind::Call(counter_address);
            tx.data = increment_calldata.into();
            tx.value = U256::ZERO;
        })
        .build_mainnet();

    // transact and commit this transaction to the database!
    evm.replay_commit().unwrap();

    let getcount_calldata = Counter::getCountCall {}.abi_encode();
    evm.modify_tx(|tx| tx.data = getcount_calldata.into());

    let ref_tx = evm.replay().unwrap().result;

    let output = match ref_tx {
        ExecutionResult::Success { output, .. } => output,
        result => return Err(anyhow!("'swap' execution failed: {result:?}")),
    };
    let decoded_output = <U256>::abi_decode(output.data()).unwrap();
    println!("Counter after increment = {}", decoded_output);

    Ok(())
}
