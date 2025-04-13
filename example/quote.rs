use alloy_primitives::{address, U256};
use alloy_sol_types::{SolCall, SolValue, sol};
use eyre::anyhow;
use eyre::Result;
use node_db::NodeDB;
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

#[tokio::main]

async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    // on-chain addresses
    let vitalik = address!("d8dA6BF26964aF9D7eEd9e03E53415D37aA96045");
    let weth = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
    let usdc = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
    let uniswap = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");

    // setup the db
    let database_path: String = std::env::var("DB_PATH").unwrap().parse().unwrap();
    let mut nodedb = NodeDB::new(database_path).unwrap();

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
            tx.caller = vitalik;
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
    let decoded_outputs = <Vec<U256>>::abi_decode(output.data(), false).unwrap();
    println!("1 WETH equals {} USDC", decoded_outputs.get(1).unwrap());

    Ok(())
}
