use codex_core::ModelProviderInfo;
use codex_core::WireApi;
use codex_core::features::Feature;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::ev_apply_patch_custom_tool_call;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::sse;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::body_string_contains;
use wiremock::matchers::method;
use wiremock::matchers::path;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continue_after_stream_error() {
    skip_if_no_network!();

    let server = MockServer::start().await;

    let fail = ResponseTemplate::new(500)
        .insert_header("content-type", "application/json")
        .set_body_string(
            serde_json::json!({
                "error": {"type": "bad_request", "message": "synthetic client error"}
            })
            .to_string(),
        );

    // The provider below disables request retries (request_max_retries = 0),
    // so the failing request should only occur once.
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .and(body_string_contains("first message"))
        .respond_with(fail)
        .up_to_n_times(2)
        .mount(&server)
        .await;

    let ok = ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_raw(
            sse(vec![
                ev_response_created("resp_ok2"),
                ev_completed("resp_ok2"),
            ]),
            "text/event-stream",
        );

    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .and(body_string_contains("follow up"))
        .respond_with(ok)
        .expect(1)
        .mount(&server)
        .await;

    // Configure a provider that uses the Responses API and points at our mock
    // server. Use an existing env var (PATH) to satisfy the auth plumbing
    // without requiring a real secret.
    let provider = ModelProviderInfo {
        name: "mock-openai".into(),
        base_url: Some(format!("{}/v1", server.uri())),
        env_key: Some("PATH".into()),
        env_key_instructions: None,
        experimental_bearer_token: None,
        wire_api: WireApi::Responses,
        query_params: None,
        http_headers: None,
        env_http_headers: None,
        request_max_retries: Some(1),
        stream_max_retries: Some(1),
        stream_idle_timeout_ms: Some(2_000),
        requires_openai_auth: false,
        supports_websockets: false,
    };

    let TestCodex { codex, .. } = test_codex()
        .with_config(move |config| {
            config.base_instructions = Some("You are a helpful assistant".to_string());
            config.model_provider = provider;
        })
        .build(&server)
        .await
        .unwrap();

    codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "first message".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
        })
        .await
        .unwrap();

    // Expect an Error followed by TurnComplete so the session is released.
    wait_for_event(&codex, |ev| matches!(ev, EventMsg::Error(_))).await;

    wait_for_event(&codex, |ev| matches!(ev, EventMsg::TurnComplete(_))).await;

    // 2) Second turn: now send another prompt that should succeed using the
    // mock server SSE stream. If the agent failed to clear the running task on
    // error above, this submission would be rejected/queued indefinitely.
    codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "follow up".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
        })
        .await
        .unwrap();

    wait_for_event(&codex, |ev| matches!(ev, EventMsg::TurnComplete(_))).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_error_records_aborted_output_for_in_flight_tool() {
    skip_if_no_network!();
    core_test_support::codex_linux_sandbox_exe_or_skip!();

    let call_id = "call-stream-error-sleep";
    let args = serde_json::json!({
        "command": "sleep 60",
        "timeout_ms": 60_000
    })
    .to_string();

    let first_body = sse(vec![
        ev_response_created("resp_stream_error_tool"),
        ev_function_call(call_id, "shell_command", &args),
    ]);
    let follow_up_body = sse(vec![
        ev_response_created("resp_follow_up"),
        ev_completed("resp_follow_up"),
    ]);

    let server = MockServer::start().await;
    let response_mock =
        core_test_support::responses::mount_sse_sequence(&server, vec![first_body, follow_up_body])
            .await;

    let fixture = test_codex()
        .with_model("gpt-5.1")
        .build(&server)
        .await
        .unwrap();
    let codex = fixture.codex;

    codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "start a tool and then break the stream".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
        })
        .await
        .unwrap();

    wait_for_event(&codex, |ev| matches!(ev, EventMsg::TurnComplete(_))).await;

    let requests = response_mock.requests();
    assert_eq!(
        requests.len(),
        2,
        "expected the broken stream to trigger a retry request"
    );

    let retry_request = &requests[1];
    assert!(
        retry_request.has_function_call(call_id),
        "expected the retry request to include the original tool call"
    );
    let output = retry_request
        .function_call_output_text(call_id)
        .expect("missing function_call_output in retry request");
    assert!(
        output.contains("aborted by user"),
        "expected aborted output after stream error, got {output:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_error_records_output_for_in_flight_apply_patch_custom_tool() {
    skip_if_no_network!();

    let call_id = "call-stream-error-apply-patch";
    let patch = [
        "*** Begin Patch",
        "*** Add File: stream_error_apply_patch.txt",
        "+hello from apply_patch",
        "*** End Patch",
    ]
    .join("\n");

    let first_body = sse(vec![
        ev_response_created("resp_stream_error_apply_patch"),
        ev_apply_patch_custom_tool_call(call_id, &patch),
    ]);
    let follow_up_body = sse(vec![
        ev_response_created("resp_follow_up"),
        ev_completed("resp_follow_up"),
    ]);

    let server = MockServer::start().await;
    let response_mock =
        core_test_support::responses::mount_sse_sequence(&server, vec![first_body, follow_up_body])
            .await;

    let fixture = test_codex()
        .with_model("gpt-5.1")
        .with_config(|config| {
            config
                .features
                .enable(Feature::ApplyPatchFreeform)
                .expect("test config should allow feature update");
        })
        .build(&server)
        .await
        .unwrap();
    let codex = fixture.codex;

    codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "start apply_patch and then break the stream".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
        })
        .await
        .unwrap();

    wait_for_event(&codex, |ev| matches!(ev, EventMsg::TurnComplete(_))).await;

    let requests = response_mock.requests();
    assert_eq!(
        requests.len(),
        2,
        "expected the broken stream to trigger a retry request"
    );

    let retry_request = &requests[1];
    let has_custom_tool_call = retry_request.input().iter().any(|item| {
        item.get("type").and_then(serde_json::Value::as_str) == Some("custom_tool_call")
            && item.get("call_id").and_then(serde_json::Value::as_str) == Some(call_id)
            && item.get("name").and_then(serde_json::Value::as_str) == Some("apply_patch")
    });
    assert!(
        has_custom_tool_call,
        "expected the retry request to include the original apply_patch call"
    );

    let (output, _success) = retry_request
        .custom_tool_call_output_content_and_success(call_id)
        .expect("missing custom_tool_call_output in retry request");
    let output = output.expect("missing apply_patch output content");
    assert!(
        !output.trim().is_empty(),
        "expected non-empty apply_patch output after stream error"
    );
}
