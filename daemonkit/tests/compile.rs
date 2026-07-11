#[test]
fn mode_and_controller_capabilities_are_enforced_by_types() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/api_inference.rs");
    tests.pass("tests/ui/child_api.rs");
    tests.pass("tests/ui/blocking_api.rs");
    tests.pass("tests/ui/custom_transport.rs");
    tests.compile_fail("tests/ui/embedded_no_endpoint.rs");
    tests.compile_fail("tests/ui/managed_no_connect.rs");
    tests.compile_fail("tests/ui/client_no_ensure.rs");
}
