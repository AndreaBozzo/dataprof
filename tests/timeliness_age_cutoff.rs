//! Whole-date freshness cutoffs. Python twin: test_timeliness_age_cutoff.py.
#[path = "common/profiling_routes.rs"]
mod profiling_routes;
#[test]
fn every_path_compares_whole_dates_for_staleness() {
    let today = chrono::Utc::now().date_naive();
    let days = (5.0_f64 * 365.2425).floor() as u64;
    let cutoff = today.checked_sub_days(chrono::Days::new(days)).unwrap();
    let values = vec![
        cutoff.pred_opt().unwrap().to_string(),
        cutoff.to_string(),
        cutoff.succ_opt().unwrap().to_string(),
        today.to_string(),
    ];
    let mut wrong = Vec::new();
    for (route, report) in profiling_routes::reports(&values) {
        let metrics = report.quality.unwrap().metrics.timeliness.unwrap();
        if metrics.stale_data_ratio != 25.0 || metrics.date_values_checked != 4 {
            wrong.push(format!(
                "[{route}] checked={}, ratio={}",
                metrics.date_values_checked, metrics.stale_data_ratio
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
