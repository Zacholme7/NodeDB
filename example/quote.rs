use std::path::Path;
use alloy_primitives::{address, U256, Address};
use alloy_sol_types::{sol, SolCall, SolValue};
use eyre::anyhow;
use eyre::Result;
use node_db::NodeDB;
use node_db::RethBackend;
use revm::context::result::ExecutionResult;
use revm::primitives::TxKind;
use revm::{Context, ExecuteEvm, MainBuilder, MainContext};

// Balance of function signature
sol!(
    #[sol(rpc)]
    contract Quote {
        function getAmountsOut(uint amountIn, address[] memory path) public view returns (uint[] memory amounts);
    }
);

fn main() -> Result<()> {
    dotenv::dotenv().ok();

    // on-chain addresses
    let weth = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
    let usdc = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
    let uniswap = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");

    // setup the db
    let database_path: String = std::env::var("DB_PATH").unwrap();
    let backend =
        RethBackend::new(Path::new(database_path.as_str())).expect("failed to open Reth database");
    let mut nodedb = NodeDB::new(backend);

    // construct the calldata for weth->usdc quote
    let out_call = Quote::getAmountsOutCall {
        amountIn: U256::from(1e16),
        path: vec![weth, usdc],
    }
    .abi_encode();

    // create evm instance and transact
    let mut evm = Context::mainnet()
        .with_db(&mut nodedb)
        .modify_tx_chained(|tx| {
            tx.caller = Address::ZERO;
            tx.value = U256::ZERO;
            tx.kind = TxKind::Call(uniswap);
            tx.data = out_call.into();
        })
        .build_mainnet();

    let ref_tx = evm.replay().unwrap().result;
    let output = match ref_tx {
        ExecutionResult::Success { output, .. } => output,
        result => return Err(anyhow!("'swap' execution failed: {result:?}")),
    };
    let decoded_outputs = <Vec<U256>>::abi_decode(output.data()).unwrap();
    println!("1 WETH equals {} USDC", decoded_outputs.get(1).unwrap());

    Ok(())
}
