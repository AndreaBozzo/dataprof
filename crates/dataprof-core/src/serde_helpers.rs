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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::Quartiles;
    use serde::Serialize;
    use serde_json::json;

    #[derive(Serialize)]
    struct Round2Value {
        #[serde(serialize_with = "round_2")]
        value: f64,
    }

    #[derive(Serialize)]
    struct Round4Value {
        #[serde(serialize_with = "round_4")]
        value: f64,
    }

    #[derive(Serialize)]
    struct Round2OptValue {
        #[serde(serialize_with = "round_2_opt")]
        value: Option<f64>,
    }

    #[derive(Serialize)]
    struct Round4OptValue {
        #[serde(serialize_with = "round_4_opt")]
        value: Option<f64>,
    }

    #[derive(Serialize)]
    struct QuartilesValue {
        #[serde(serialize_with = "quartiles::serialize")]
        value: Option<Quartiles>,
    }

    #[test]
    fn test_round_2_serializes_rounded_value() {
        let value = Round2Value { value: 12.345 };
        let json = serde_json::to_value(value).unwrap();
        assert_eq!(json, json!({ "value": 12.35 }));
    }

    #[test]
    fn test_round_4_serializes_rounded_value() {
        let value = Round4Value { value: 12.34567 };
        let json = serde_json::to_value(value).unwrap();
        assert_eq!(json, json!({ "value": 12.3457 }));
    }

    #[test]
    fn test_non_finite_values_serialize_as_null() {
        let round_2_json = serde_json::to_value(Round2Value { value: f64::NAN }).unwrap();
        let round_4_json = serde_json::to_value(Round4Value {
            value: f64::INFINITY,
        })
        .unwrap();

        assert_eq!(round_2_json, json!({ "value": null }));
        assert_eq!(round_4_json, json!({ "value": null }));
    }

    #[test]
    fn test_optional_rounders_handle_some_none_and_non_finite() {
        let rounded_2 = serde_json::to_value(Round2OptValue { value: Some(9.876) }).unwrap();
        let rounded_4 = serde_json::to_value(Round4OptValue {
            value: Some(9.87654),
        })
        .unwrap();
        let none_2 = serde_json::to_value(Round2OptValue { value: None }).unwrap();
        let nan_4 = serde_json::to_value(Round4OptValue {
            value: Some(f64::NAN),
        })
        .unwrap();

        assert_eq!(rounded_2, json!({ "value": 9.88 }));
        assert_eq!(rounded_4, json!({ "value": 9.8765 }));
        assert_eq!(none_2, json!({ "value": null }));
        assert_eq!(nan_4, json!({ "value": null }));
    }

    #[test]
    fn test_quartiles_serializer_rounds_each_field() {
        let value = QuartilesValue {
            value: Some(Quartiles {
                q1: 1.234,
                q2: 2.345,
                q3: 3.456,
                iqr: 2.222,
            }),
        };
        let json = serde_json::to_value(value).unwrap();

        assert_eq!(
            json,
            json!({
                "value": {
                    "q1": 1.23,
                    "q2": 2.35,
                    "q3": 3.46,
                    "iqr": 2.22
                }
            })
        );
    }

    #[test]
    fn test_quartiles_serializer_handles_none() {
        let value = QuartilesValue { value: None };
        let json = serde_json::to_value(value).unwrap();
        assert_eq!(json, json!({ "value": null }));
    }
}
