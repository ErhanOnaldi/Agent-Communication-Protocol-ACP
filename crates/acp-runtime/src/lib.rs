pub mod adapter;
pub mod manager;
pub mod output;

pub use adapter::claudex::ClaudexProvider;
pub use adapter::{ProcessRuntimeAdapter, RuntimeAdapter};
pub use manager::RuntimeManager;
pub use output::{classify_output, parse_stream_json_events};

use acp_protocol::RuntimeType;

pub fn adapter_for(runtime_type: RuntimeType) -> ProcessRuntimeAdapter {
    match runtime_type {
        RuntimeType::ClaudeCode => ProcessRuntimeAdapter::external(runtime_type, "claude"),
        RuntimeType::Codex => ProcessRuntimeAdapter::external(runtime_type, "codex"),
        RuntimeType::Gemini => ProcessRuntimeAdapter::external(runtime_type, "gemini"),
        RuntimeType::Copilot => ProcessRuntimeAdapter::external(runtime_type, "copilot"),
        RuntimeType::Claudex => ProcessRuntimeAdapter::claudex(ClaudexProvider::from_env()),
    }
}
