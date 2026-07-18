//! Subscribes to component updates and defers each one by 30s (demonstrating the update-defer flow).
//!
//! In a real component you would only defer while "busy" and acknowledge (recheckAfterMs = 0)
//! otherwise. Note that, unlike the official AWS C SDK, IPC calls (like `defer_component_update`)
//! are safe to make directly from inside the subscription loop.

use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let client = greengrass_ipc::Client::connect_from_env().await?;
    client
        .update_state(greengrass_ipc::LifecycleState::Running)
        .await?;

    let mut updates = client.subscribe_to_component_updates().await?;
    while let Some(event) = updates.next().await {
        let event = event?;
        if let Some(pre) = event.pre_update_event {
            println!("deferring deployment {}", pre.deployment_id);
            client
                .defer_component_update(pre.deployment_id, Some(30_000), None)
                .await?;
        }
    }
    Ok(())
}
