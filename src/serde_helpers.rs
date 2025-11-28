//! Custom serde serialization helpers for formatting numeric values with appropriate precision

use serde::Serializer;

/// Round f64 to 2 decimal places (for percentages and simple ratios)
/// Returns null for NaN or infinite values
pub fn round_2<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if !value.is_finite() {
        return serializer.serialize_none();
    }
    serializer.serialize_f64((value * 100.0).round() / 100.0)
}

/// Round f64 to 4 decimal places (for statistical metrics like mean, std_dev)
/// Returns null for NaN or infinite values
pub fn round_4<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if !value.is_finite() {
        return serializer.serialize_none();
    }
    serializer.serialize_f64((value * 10000.0).round() / 10000.0)
}

/// Round Option<f64> to 2 decimal places
/// Returns null for None or non-finite values
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

/// Round Option<f64> to 4 decimal places
/// Returns null for None or non-finite values
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

/// Round Quartiles fields to 2 decimal places
pub mod quartiles {
    use super::*;
    use crate::types::Quartiles;

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
    use serde::Serialize;
    use serde_json;

    #[test]
    fn test_round_2() {
        #[derive(Serialize)]
        struct Test {
            #[serde(serialize_with = "round_2")]
            value: f64,
        }

        let test = Test {
            value: 6.666666666666667,
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"value":6.67}"#);
    }

    #[test]
    fn test_round_4() {
        #[derive(Serialize)]
        struct Test {
            #[serde(serialize_with = "round_4")]
            value: f64,
        }

        let test = Test {
            value: 5.466666666666667,
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"value":5.4667}"#);
    }

    #[test]
    fn test_round_4_opt_some() {
        #[derive(Serialize)]
        struct Test {
            #[serde(serialize_with = "round_4_opt")]
            value: Option<f64>,
        }

        let test = Test {
            value: Some(14.290634073484004),
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"value":14.2906}"#);
    }

    #[test]
    fn test_round_4_opt_none() {
        #[derive(Serialize)]
        struct Test {
            #[serde(serialize_with = "round_4_opt")]
            value: Option<f64>,
        }

        let test = Test { value: None };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"value":null}"#);
    }

    #[test]
    fn test_enum_variant_rounding() {
        #[derive(Serialize)]
        enum TestEnum {
            Numeric {
                #[serde(serialize_with = "round_2")]
                value: f64,
            },
        }

        let test = TestEnum::Numeric {
            value: 6.666666666666667,
        };
        let json = serde_json::to_string(&test).unwrap();
        println!("Enum JSON: {}", json);
        assert!(json.contains("6.67"));
    }

    #[test]
    fn test_round_2_nan() {
        #[derive(Serialize)]
        struct Test {
            #[serde(serialize_with = "round_2")]
            value: f64,
        }

        let test = Test { value: f64::NAN };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"value":null}"#);
    }

    #[test]
    fn test_round_2_infinity() {
        #[derive(Serialize)]
        struct Test {
            #[serde(serialize_with = "round_2")]
            value: f64,
        }

        let test = Test {
            value: f64::INFINITY,
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"value":null}"#);
    }

    #[test]
    fn test_round_4_neg_infinity() {
        #[derive(Serialize)]
        struct Test {
            #[serde(serialize_with = "round_4")]
            value: f64,
        }

        let test = Test {
            value: f64::NEG_INFINITY,
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"value":null}"#);
    }

    #[test]
    fn test_round_2_opt_nan() {
        #[derive(Serialize)]
        struct Test {
            #[serde(serialize_with = "round_2_opt")]
            value: Option<f64>,
        }

        let test = Test {
            value: Some(f64::NAN),
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"value":null}"#);
    }

    #[test]
    fn test_round_4_opt_infinity() {
        #[derive(Serialize)]
        struct Test {
            #[serde(serialize_with = "round_4_opt")]
            value: Option<f64>,
        }

        let test = Test {
            value: Some(f64::INFINITY),
        };
        let json = serde_json::to_string(&test).unwrap();
        assert_eq!(json, r#"{"value":null}"#);
    }
}
