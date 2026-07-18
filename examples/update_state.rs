//! Reports the component as RUNNING to the Greengrass nucleus, then exits.
//!
//! Run this as the `Startup` (or `Run`) script of a Greengrass generic component.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let client = greengrass_ipc::Client::connect_from_env().await?;
    client
        .update_state(greengrass_ipc::LifecycleState::Running)
        .await?;
    println!("reported RUNNING to the nucleus");
    Ok(())
}
