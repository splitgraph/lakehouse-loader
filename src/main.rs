use clap::Parser;
use lakehouse_loader::{do_main, error::DataLoadingError, Cli};

#[tokio::main]
async fn main() -> Result<(), DataLoadingError> {
    env_logger::init();
    let args = Cli::parse();
    do_main(args).await
}
