use alloy_primitives::{address, U256};
use alloy_sol_types::{SolCall, SolValue, sol};
use eyre::anyhow;
use eyre::Result;
use node_db::{InsertionType, NodeDB};
use revm::context::result::ExecutionResult;
use revm::primitives::{keccak256, TxKind};
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

// Appoval and Allowance function signatures
sol!(
    #[sol(rpc)]
    contract ERC20Token {
        function approve(address spender, uint256 amount) external returns (bool success);
        function allowance(address owner, address spender) public view returns (uint256);
    }
);

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    // on chain addresses
    let account = address!("0000000000000000000000000000000000000001");
    let uniswap_router = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
    let weth = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");

    // construct the database
    let database_path = std::env::var("DB_PATH").unwrap().parse().unwrap();
    let mut nodedb = NodeDB::new(database_path).unwrap();

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
            tx.kind = TxKind::Call(weth);
            tx.value = U256::ZERO;
            tx.data = approve_calldata.into();
        })
        .build_mainnet();

    evm.replay_commit().unwrap();

    // now confirm the allowance for the router
    let allowance_calldata = ERC20Token::allowanceCall {
        owner: account,
        spender: uniswap_router,
    }
    .abi_encode();

    let mut evm = Context::mainnet()
        .with_db(&mut nodedb)
        .modify_tx_chained(|tx| {
            tx.caller = account;
            tx.kind = TxKind::Call(weth);
            tx.value = U256::ZERO;
            tx.data = allowance_calldata.into();
        })
        .build_mainnet();

    let ref_tx = evm.replay_commit().unwrap();

    let output = match ref_tx {
        ExecutionResult::Success { output, .. } => output,
        result => return Err(anyhow!("'swap' execution failed: {result:?}")),
    };

    let allowance = <U256>::abi_decode(output.data(), false).unwrap();
    println!("The router is allowed to spend {:?} weth", allowance);

    Ok(())
}
