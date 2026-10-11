//! Row-only trees have the same cap across routes. Python twin: test_memory_row_limits.py.
#[path = "common/profiling_routes.rs"]
mod profiling_routes;
use dataprof::StopCondition;
#[test]
fn row_limit_trees_apply_to_every_input_route() {
    let max = StopCondition::MaxRows;
    let cases = [
        ("leaf", max(3), 3),
        ("any", StopCondition::Any(vec![max(3), max(7)]), 3),
        ("all", StopCondition::All(vec![max(3), max(7)]), 7),
        (
            "nested",
            StopCondition::All(vec![StopCondition::Any(vec![max(3), max(7)]), max(5)]),
            5,
        ),
        ("never", StopCondition::Never, 20),
    ];
    let values = (0..20).map(|i| i.to_string()).collect::<Vec<_>>();
    let mut wrong = Vec::new();
    for (case, stop, expected) in cases {
        for (route, report) in profiling_routes::reports_with_stop(&values, stop) {
            if report.execution.rows_processed != expected {
                wrong.push(format!(
                    "[{route}] {case}: {}, expected {expected}",
                    report.execution.rows_processed
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
