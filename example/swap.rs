use std::path::Path;

use alloy_primitives::{address, U256};
use alloy_sol_types::{sol, SolCall, SolValue};
use eyre::anyhow;
use eyre::Result;
use node_db::NodeDBStorageSync;
use node_db::RethBackend;
use node_db::{InsertionType, NodeDB};
use revm::context::result::ExecutionResult;
use revm::primitives::{keccak256, TxKind};
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

sol!(
    #[sol(rpc)]
    contract V2Swap {
        function swapExactTokensForTokens(
            uint256 amountIn,
            uint256 amountOutMin,
            address[] calldata path,
            address to,
            uint256 deadline
        ) external returns (uint256[] memory amounts);
    }
);

sol!(
    #[sol(rpc)]
    contract ERC20Token {
        function approve(address spender, uint256 amount) external returns (bool success);
    }
);

fn main() -> Result<()> {
    dotenv::dotenv().ok();

    // on chain addresses
    let account = address!("0000000000000000000000000000000000000001");
    let uniswap_router = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
    let weth = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
    let usdc = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");

    // construct the database
    let database_path: String = std::env::var("DB_PATH").unwrap();
    let backend =
        RethBackend::new(Path::new(database_path.as_str())).expect("failed to open Reth database");
    let mut nodedb = NodeDB::new(backend);

    // give our account some weth
    let balance_slot = keccak256((account, U256::from(3)).abi_encode());
    nodedb.insert_account_storage(
        weth,
        balance_slot.into(),
        U256::from(1e18),
        InsertionType::OnChain, // weth has a corresponding onchain contract
    )?;

    // setup approval call and commit the transaction
    let approve_calldata = ERC20Token::approveCall {
        spender: uniswap_router,
        amount: U256::from(10e18),
    }
    .abi_encode();
    // construct a new evm instance
    let mut evm = Context::mainnet()
        .with_db(&mut nodedb)
        .modify_tx_chained(|tx| {
            tx.caller = account;
            tx.value = U256::ZERO;
            tx.kind = TxKind::Call(weth);
            tx.data = approve_calldata.into();
        })
        .build_mainnet();

    evm.replay_commit().unwrap();

    // we now have some of the input token and we have approved the router to spend it

    // setup calldata based on the swap type
    let calldata = V2Swap::swapExactTokensForTokensCall {
        amountIn: U256::from(1e18),
        amountOutMin: U256::ZERO,
        path: vec![weth, usdc],
        to: account,
        deadline: U256::MAX,
    }
    .abi_encode();
    evm.modify_tx(|tx| {
        tx.data = calldata.into();
        tx.kind = TxKind::Call(uniswap_router);
    });

    // if we can transact, add it as it is a valid pool. Else ignore it
    let ref_tx = evm.replay_commit().unwrap();
    let output = match ref_tx {
        ExecutionResult::Success { output, .. } => output,
        result => return Err(anyhow!("'swap' execution failed: {result:?}")),
    };

    let decoded_outputs = <Vec<U256>>::abi_decode(output.data()).unwrap();
    println!(
        "Swapped 1 WETH for {} USDC",
        decoded_outputs.get(1).unwrap()
    );

    Ok(())
}
