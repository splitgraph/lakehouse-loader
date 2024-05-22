use clap::Parser;
use lakehouse_loader::{do_main, Cli, DataLoadingError};

#[tokio::main]
async fn main() -> Result<(), DataLoadingError> {
    env_logger::init();
    let args = Cli::parse();
    do_main(args).await
}
