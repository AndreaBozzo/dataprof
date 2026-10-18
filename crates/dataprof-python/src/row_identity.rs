use dataprof::{QualityAssessment, QualityMetrics, ScoreBounds, ScoreInterval};
use pyo3::prelude::*;

use super::types::PyProfileReport;

fn shift_interval(interval: &mut ScoreInterval, delta: f64) {
    interval.lower = (interval.lower + delta).clamp(0.0, 100.0);
    interval.upper = (interval.upper + delta).clamp(0.0, 100.0);
}

fn adjusted_score_bounds(
    mut bounds: Option<ScoreBounds>,
    old_metrics: &QualityMetrics,
    new_metrics: &QualityMetrics,
    old_approximate: bool,
    new_approximate: bool,
) -> Option<ScoreBounds> {
    let Some(bounds) = bounds.as_mut() else {
        return None;
    };

    let old_uniqueness = old_metrics.uniqueness_score();
    let new_uniqueness = new_metrics.uniqueness_score();
    if old_uniqueness == new_uniqueness && old_approximate == new_approximate {
        return Some(bounds.clone());
    }

    if !old_approximate && !new_approximate {
        if let (Some(old_score), Some(new_score)) = (old_uniqueness, new_uniqueness) {
            let delta = new_score - old_score;
            if let Some(Some(interval)) = bounds.dimension_scores.get_mut("uniqueness") {
                shift_interval(interval, delta);
            }

            if let (Some(old_overall), Some(new_overall), Some(interval)) = (
                old_metrics.overall_score(),
                new_metrics.overall_score(),
                bounds.overall_score.as_mut(),
            ) {
                shift_interval(interval, new_overall - old_overall);
            }
            return Some(bounds.clone());
        }
    }

    // An approximate duplicate count has no usable floor in QualityMetrics,
    // so the uniqueness interval cannot be reconstructed safely here. Keep the
    // result conservative rather than attaching the old interval to new data.
    bounds
        .dimension_scores
        .insert("uniqueness".to_string(), None);
    if new_metrics.score_weights.uniqueness > 0.0 && new_uniqueness.is_some() {
        bounds.overall_score = None;
    }
    Some(bounds.clone())
}

/// Replace only the full-row duplicate evidence on a freshly profiled report.
///
/// Pandas' mixed-object Arrow fallback stringifies containers. A second,
/// container-aware row-signature pass can recover the correct duplicate count,
/// while this helper keeps every native Arrow column profile and every other
/// quality component from the original report. Rebuilding the assessment also
/// invalidates cached scores derived from the old duplicate count; score bounds
/// are shifted when the duplicate component remains exact and conservatively
/// withheld when an approximate count makes that impossible.
#[pyfunction(name = "_replace_row_duplicates")]
pub fn replace_row_duplicates(
    mut report: PyRefMut<'_, PyProfileReport>,
    duplicate_rows: usize,
    rows_checked: usize,
    approximate: bool,
) -> PyResult<()> {
    let Some(quality) = report.inner.quality.take() else {
        return Ok(());
    };
    let Some(current) = quality.metrics.uniqueness.as_ref() else {
        report.inner.quality = Some(quality);
        return Ok(());
    };

    if current.duplicate_rows == duplicate_rows
        && current.rows_checked == rows_checked
        && current.duplicate_rows_approximate == approximate
    {
        report.inner.quality = Some(quality);
        return Ok(());
    }

    let old_metrics = quality.metrics.clone();
    let old_approximate = current.duplicate_rows_approximate;
    let old_bounds = quality.score_bounds().cloned();
    let confidence = quality.confidence.clone();
    let mut new_metrics = quality.metrics;

    let uniqueness = new_metrics
        .uniqueness
        .as_mut()
        .expect("checked above that uniqueness is present");
    uniqueness.duplicate_rows = duplicate_rows;
    uniqueness.rows_checked = rows_checked;
    uniqueness.duplicate_rows_approximate = approximate;

    let bounds = adjusted_score_bounds(
        old_bounds,
        &old_metrics,
        &new_metrics,
        old_approximate,
        approximate,
    );
    report.inner.quality = Some(
        QualityAssessment::new(new_metrics, confidence).with_score_bounds(bounds),
    );
    Ok(())
}
