use acp_protocol::{RuntimeHealth, RuntimeStreamEvent};

pub fn parse_stream_json_events(stdout: &str) -> Vec<RuntimeStreamEvent> {
    stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .map(|payload| {
            let event_type = payload
                .get("type")
                .or_else(|| payload.get("event"))
                .and_then(|value| value.as_str())
                .unwrap_or("runtime_event")
                .to_string();
            RuntimeStreamEvent {
                event_type,
                payload,
            }
        })
        .collect()
}

pub fn classify_output(exit_code: Option<i32>, stdout: &str, stderr: &str) -> RuntimeHealth {
    let combined = format!("{stdout}\n{stderr}").to_lowercase();
    if combined.contains("rate limit")
        || combined.contains("rate_limit")
        || combined.contains("too many requests")
        || combined.contains("429")
    {
        RuntimeHealth::RateLimited
    } else if combined.contains("auth")
        || combined.contains("unauthorized")
        || combined.contains("invalid api key")
        || combined.contains("401")
    {
        RuntimeHealth::AuthExpired
    } else if exit_code.is_some_and(|code| code != 0) {
        RuntimeHealth::Crashed
    } else {
        RuntimeHealth::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_rate_limits_before_crashes() {
        assert_eq!(
            classify_output(Some(1), "", "429 too many requests: rate limit"),
            RuntimeHealth::RateLimited
        );
    }

    #[test]
    fn classifies_auth_failures() {
        assert_eq!(
            classify_output(Some(1), "", "401 unauthorized invalid api key"),
            RuntimeHealth::AuthExpired
        );
    }

    #[test]
    fn parses_line_delimited_stream_json() {
        let events = parse_stream_json_events(
            r#"{"type":"assistant","message":"hi"}
not json
{"event":"tool_result","ok":true}"#,
        );
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "assistant");
        assert_eq!(events[1].event_type, "tool_result");
    }
}
