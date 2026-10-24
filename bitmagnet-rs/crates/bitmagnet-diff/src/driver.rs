use anyhow::Result;
use serde_json::Value;

/// Runs one subsystem implementation against fixture input.
pub trait Driver {
    fn subsystem(&self) -> &str;
    fn run(&self, input: &Value) -> Result<Value>;
}
