//! Custom serde serialization helpers for formatting numeric values with appropriate precision.

use serde::Serializer;

/// Round f64 to 2 decimal places (for percentages and simple ratios).
/// Returns null for NaN or infinite values.
pub fn round_2<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if !value.is_finite() {
        return serializer.serialize_none();
    }
    serializer.serialize_f64((value * 100.0).round() / 100.0)
}

/// Round f64 to 4 decimal places (for statistical metrics like mean, std_dev).
/// Returns null for NaN or infinite values.
pub fn round_4<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if !value.is_finite() {
        return serializer.serialize_none();
    }
    serializer.serialize_f64((value * 10000.0).round() / 10000.0)
}

/// Round `Option<f64>` to 2 decimal places.
/// Returns null for None or non-finite values.
pub fn round_2_opt<S>(value: &Option<f64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(v) if v.is_finite() => {
            let rounded = (v * 100.0).round() / 100.0;
            serializer.serialize_some(&rounded)
        }
        _ => serializer.serialize_none(),
    }
}

/// Round `Option<f64>` to 4 decimal places.
/// Returns null for None or non-finite values.
pub fn round_4_opt<S>(value: &Option<f64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(v) if v.is_finite() => {
            let rounded = (v * 10000.0).round() / 10000.0;
            serializer.serialize_some(&rounded)
        }
        _ => serializer.serialize_none(),
    }
}

/// Round Quartiles fields to 2 decimal places.
pub mod quartiles {
    use super::*;
    use crate::profile::Quartiles;

    pub fn serialize<S>(value: &Option<Quartiles>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(q) => {
                use serde::Serialize;

                #[derive(Serialize)]
                struct RoundedQuartiles {
                    #[serde(serialize_with = "round_2")]
                    q1: f64,
                    #[serde(serialize_with = "round_2")]
                    q2: f64,
                    #[serde(serialize_with = "round_2")]
                    q3: f64,
                    #[serde(serialize_with = "round_2")]
                    iqr: f64,
                }

                let rounded = RoundedQuartiles {
                    q1: q.q1,
                    q2: q.q2,
                    q3: q.q3,
                    iqr: q.iqr,
                };
                serializer.serialize_some(&rounded)
            }
            None => serializer.serialize_none(),
        }
    }
}