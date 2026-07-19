//! Minimal Greengrass component that uses `greengrass-ipc` (from crates.io).
//!
//! It connects to the Greengrass nucleus over IPC, reports the component as
//! `RUNNING`, then stays alive with a periodic heartbeat so the component shows
//! `RUNNING` in `greengrass-cli component list`. This proves the published crate
//! works against a real nucleus.

use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let client = greengrass_ipc::Client::connect_from_env().await?;
    client
        .update_state(greengrass_ipc::LifecycleState::Running)
        .await?;
    println!("greengrass-ipc-demo: connected to the nucleus and reported RUNNING");

    // Keep the process alive so the nucleus supervises it as RUNNING.
    let mut ticks: u64 = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
        ticks += 1;
        println!("greengrass-ipc-demo: alive (heartbeat #{ticks})");
    }
}
