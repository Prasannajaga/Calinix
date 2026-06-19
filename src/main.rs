#[tokio::main]
async fn main() {
    if let Err(err) = calinix::app::bootstrap::run_from_cli().await {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}
