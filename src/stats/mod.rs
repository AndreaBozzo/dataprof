pub mod datetime {
    pub use dataprof_metrics::stats::datetime::*;
}

pub mod numeric {
    pub use dataprof_metrics::stats::numeric::*;
}

pub mod text {
    pub use dataprof_metrics::stats::text::*;
}

pub use datetime::calculate_datetime_stats;
pub use numeric::calculate_numeric_stats;
pub use text::calculate_text_stats;
