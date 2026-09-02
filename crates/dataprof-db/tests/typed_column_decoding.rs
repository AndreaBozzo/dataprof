//! Types that once fell past every arm of `db_column_to_string!` now decode.
//!
//! Until 0.12 the chain tried `String`, the integers, the floats and `bool`,
//! then gave up and recorded `None`, which the profiler cannot tell from SQL
//! NULL. Temporal columns, `NUMERIC`/`DECIMAL` and `UUID` all landed there, so
//! a database source reported them as entirely null. MySQL's `BIGINT UNSIGNED`
//! was worse: it reached the `bool` arm and reported `true` (#365).
//!
//! Temporal columns were the costly case. The Timeliness ISO dimension is
//! computed from date values, so it assessed nothing at all on any database
//! source. A dimension that measures nothing is exactly what
//! `None`-means-not-analyzed exists to make visible, and it stayed that way
//! until the values arrived.
//!
//! These tests pin the decoded forms: the rendering each type produces, that
//! SQL NULL still reads as NULL underneath the new arms, and that the batch and
//! streaming paths of both connectors agree.
//!
//! SQLite is absent by design. It has no native temporal, decimal or UUID
//! types, so those columns arrive as TEXT and the `String` arm already decoded
//! them; the gap was a PostgreSQL and MySQL one.

#![cfg(any(feature = "postgres", feature = "mysql"))]

/// Values of one column, in row order, with NULLs rendered as the profiler's
/// empty-string null.
fn column<'a>(columns: &'a dataprof_db::QueryColumns, name: &str) -> &'a [String] {
    columns
        .get(name)
        .unwrap_or_else(|| panic!("column {name} missing from the result"))
}

#[cfg(feature = "postgres")]
mod postgres {
    use super::column;
    use sqlx::postgres::PgPoolOptions;

    const DDL: &str = "CREATE TABLE typed_decoding_pg (
            id INTEGER,
            ts TIMESTAMP,
            tstz TIMESTAMPTZ,
            d DATE,
            t TIME,
            amount NUMERIC(12,4),
            uid UUID
        )";

    const ROWS: &str = "INSERT INTO typed_decoding_pg VALUES
            (1, '2024-01-15 10:30:00', '2024-01-15 10:30:00+00', '2024-01-15',
             '10:30:00', 1234.5678, '11111111-2222-3333-4444-555555555555'),
            (2, NULL, NULL, NULL, NULL, NULL, NULL)";

    /// Built once per process: every test reads the same profiled fixture.
    ///
    /// Each test dropping and recreating one shared table raced with the
    /// others under the default parallel runner, so a green run depended on
    /// `--test-threads=1`.
    static FIXTURE: tokio::sync::OnceCell<Option<dataprof_db::QueryColumns>> =
        tokio::sync::OnceCell::const_new();

    /// Profile through the connector, not through the decode macro directly.
    ///
    /// The backend-specific arms live at the connector's call site, so a test
    /// that expanded the macro itself would have to pass those arms in — and
    /// would then keep passing if a connector dropped them.
    async fn decoded() -> Option<&'static dataprof_db::QueryColumns> {
        FIXTURE.get_or_init(build).await.as_ref()
    }

    async fn build() -> Option<dataprof_db::QueryColumns> {
        let url = std::env::var("POSTGRES_TEST_URL").ok()?;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("postgres connection");
        sqlx::query("DROP TABLE IF EXISTS typed_decoding_pg")
            .execute(&pool)
            .await
            .expect("drop");
        sqlx::query(DDL).execute(&pool).await.expect("ddl");
        sqlx::query(ROWS).execute(&pool).await.expect("rows");

        let mut connector = dataprof_db::create_connector(dataprof_db::DatabaseConfig {
            connection_string: url,
            load_credentials_from_env: false,
            ..Default::default()
        })
        .expect("connector");
        connector.connect().await.expect("connect");
        let columns = connector
            .profile_query("SELECT * FROM typed_decoding_pg ORDER BY id")
            .await
            .expect("profile query");
        connector.disconnect().await.expect("disconnect");
        Some(columns)
    }

    #[tokio::test]
    async fn timestamps_decode_in_a_form_the_profiler_reads_as_a_date() {
        let Some(columns) = decoded().await else {
            return;
        };

        // Naive ISO, not RFC 3339. Since #643 the grammar would accept the
        // offset too, and an offset-bearing value is normalized to UTC before
        // any statistic reads it — so the two renderings profile identically
        // and the connectors keep converting to UTC here instead.
        assert_eq!(column(columns, "ts"), ["2024-01-15T10:30:00", ""]);
        assert_eq!(column(columns, "tstz"), ["2024-01-15T10:30:00", ""]);
        assert_eq!(column(columns, "d"), ["2024-01-15", ""]);
    }

    #[tokio::test]
    async fn time_of_day_decodes_even_though_it_is_not_a_date() {
        let Some(columns) = decoded().await else {
            return;
        };

        // A TIME carries no date, so it profiles as text rather than feeding
        // Timeliness. Decoding it is still better than reporting it as null.
        assert_eq!(column(columns, "t"), ["10:30:00", ""]);
    }

    #[tokio::test]
    async fn numeric_keeps_its_exact_decimal_string() {
        let Some(columns) = decoded().await else {
            return;
        };

        // Not routed through f64: NUMERIC exists precisely because binary
        // floating point cannot hold these values, and profiling the rounded
        // form would report statistics about a number the database does not
        // contain.
        assert_eq!(column(columns, "amount"), ["1234.5678", ""]);
    }

    #[tokio::test]
    async fn uuid_decodes_hyphenated() {
        let Some(columns) = decoded().await else {
            return;
        };

        assert_eq!(
            column(columns, "uid"),
            ["11111111-2222-3333-4444-555555555555", ""]
        );
    }

    #[tokio::test]
    async fn the_streaming_path_decodes_the_same_types() {
        // Each connector passes its backend arms at two call sites, the batch
        // one and the streaming one. Updating only the first is an easy miss
        // and nothing else would catch it: the two paths would then disagree
        // about what a NUMERIC column contains.
        let Some(url) = std::env::var("POSTGRES_TEST_URL").ok() else {
            return;
        };
        let _ = decoded().await;

        let mut connector = dataprof_db::create_connector(dataprof_db::DatabaseConfig {
            connection_string: url,
            load_credentials_from_env: false,
            ..Default::default()
        })
        .expect("connector");
        connector.connect().await.expect("connect");
        let columns = connector
            .profile_query_streaming("SELECT * FROM typed_decoding_pg ORDER BY id", 1)
            .await
            .expect("streaming profile");
        connector.disconnect().await.expect("disconnect");

        assert_eq!(column(&columns, "amount"), ["1234.5678", ""]);
        assert_eq!(column(&columns, "ts"), ["2024-01-15T10:30:00", ""]);
        assert_eq!(
            column(&columns, "uid"),
            ["11111111-2222-3333-4444-555555555555", ""]
        );
    }

    #[tokio::test]
    async fn a_timestamp_column_makes_timeliness_assessable() {
        // The reason this ticket is worth more than a decode fix. Timeliness is
        // computed from date values, so while every temporal column arrived as
        // null the dimension assessed nothing on any database source, and the
        // report said so honestly: absent, not zero. Decoding the column is what
        // gives the dimension something to measure.
        let Some(url) = std::env::var("POSTGRES_TEST_URL").ok() else {
            return;
        };
        // Depends on the fixture table the shared profile built.
        let _ = decoded().await;

        let report = dataprof_db::analyze_database(
            dataprof_db::DatabaseConfig {
                connection_string: url,
                load_credentials_from_env: false,
                ..Default::default()
            },
            "SELECT ts FROM typed_decoding_pg WHERE ts IS NOT NULL",
            true,
            None,
        )
        .await
        .expect("profile");

        let quality = report.quality.as_ref().expect("quality assessed");
        assert!(
            quality
                .metrics
                .assessed_dimensions()
                .contains(&dataprof_core::QualityDimension::Timeliness),
            "timeliness still assesses nothing: {:?}",
            quality.metrics.assessed_dimensions()
        );
        let timeliness = quality
            .metrics
            .timeliness
            .as_ref()
            .expect("timeliness evidence");
        assert!(
            timeliness.date_values_checked > 0,
            "timeliness reported evidence without checking a date"
        );
    }

    #[tokio::test]
    async fn a_null_row_is_still_null() {
        let Some(columns) = decoded().await else {
            return;
        };

        // The new arms must not turn SQL NULL into a rendered zero value. The
        // `Option<String>` arm is the NULL detector for every type, and adding
        // arms below it must not disturb that.
        for name in ["ts", "tstz", "d", "t", "amount", "uid"] {
            assert_eq!(column(columns, name)[1], "", "{name} lost its NULL");
        }
    }
}

#[cfg(feature = "mysql")]
mod mysql {
    use super::column;
    use sqlx::mysql::MySqlPoolOptions;

    const DDL: &str = "CREATE TABLE typed_decoding_my (
            id INT,
            dt DATETIME,
            ts TIMESTAMP NULL,
            d DATE,
            amount DECIMAL(12,4),
            big BIGINT UNSIGNED,
            t TIME(3)
        )";

    const ROWS: &str = "INSERT INTO typed_decoding_my VALUES
            (1, '2024-01-15 10:30:00', '2024-01-15 10:30:00', '2024-01-15',
             1234.5678, 18446744073709551615, '-837:59:59.123'),
            (2, NULL, NULL, NULL, NULL, NULL, NULL)";

    static FIXTURE: tokio::sync::OnceCell<Option<dataprof_db::QueryColumns>> =
        tokio::sync::OnceCell::const_new();

    /// See the PostgreSQL note: this profiles through the connector so the
    /// backend arms under test are the ones the connector actually passes.
    async fn decoded() -> Option<&'static dataprof_db::QueryColumns> {
        FIXTURE.get_or_init(build).await.as_ref()
    }

    async fn build() -> Option<dataprof_db::QueryColumns> {
        let url = std::env::var("MYSQL_TEST_URL").ok()?;
        let pool = MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("mysql connection");
        sqlx::query("DROP TABLE IF EXISTS typed_decoding_my")
            .execute(&pool)
            .await
            .expect("drop");
        sqlx::query(DDL).execute(&pool).await.expect("ddl");
        sqlx::query(ROWS).execute(&pool).await.expect("rows");

        let mut connector = dataprof_db::create_connector(dataprof_db::DatabaseConfig {
            connection_string: url,
            load_credentials_from_env: false,
            ..Default::default()
        })
        .expect("connector");
        connector.connect().await.expect("connect");
        let columns = connector
            .profile_query("SELECT * FROM typed_decoding_my ORDER BY id")
            .await
            .expect("profile query");
        connector.disconnect().await.expect("disconnect");
        Some(columns)
    }

    #[tokio::test]
    async fn temporal_columns_decode_in_a_form_the_profiler_reads_as_a_date() {
        let Some(columns) = decoded().await else {
            return;
        };

        assert_eq!(column(columns, "dt"), ["2024-01-15T10:30:00", ""]);
        assert_eq!(column(columns, "ts"), ["2024-01-15T10:30:00", ""]);
        assert_eq!(column(columns, "d"), ["2024-01-15", ""]);
    }

    #[tokio::test]
    async fn decimal_keeps_its_exact_string() {
        let Some(columns) = decoded().await else {
            return;
        };

        assert_eq!(column(columns, "amount"), ["1234.5678", ""]);
    }

    #[tokio::test]
    async fn time_of_day_decodes_even_though_it_is_not_a_date() {
        let Some(columns) = decoded().await else {
            return;
        };

        assert_eq!(column(columns, "t"), ["-837:59:59.123", ""]);
    }

    #[tokio::test]
    async fn the_streaming_path_decodes_the_same_types() {
        // MySQL passes its arms at two call sites like PostgreSQL does, and
        // only the batch one is reached by `decoded()`. Dropping the list from
        // the streaming site would otherwise leave this suite green while the
        // two public paths disagree about the same column.
        let Some(url) = std::env::var("MYSQL_TEST_URL").ok() else {
            return;
        };
        let _ = decoded().await;

        let mut connector = dataprof_db::create_connector(dataprof_db::DatabaseConfig {
            connection_string: url,
            load_credentials_from_env: false,
            ..Default::default()
        })
        .expect("connector");
        connector.connect().await.expect("connect");
        let columns = connector
            .profile_query_streaming("SELECT * FROM typed_decoding_my ORDER BY id", 1)
            .await
            .expect("streaming profile");
        connector.disconnect().await.expect("disconnect");

        assert_eq!(column(&columns, "big"), ["18446744073709551615", ""]);
        assert_eq!(column(&columns, "amount"), ["1234.5678", ""]);
        assert_eq!(column(&columns, "dt"), ["2024-01-15T10:30:00", ""]);
        assert_eq!(column(&columns, "t"), ["-837:59:59.123", ""]);
    }

    #[tokio::test]
    async fn unsigned_bigint_survives_above_i64_max() {
        let Some(columns) = decoded().await else {
            return;
        };

        // `u64` has no `Type<Postgres>` impl, so it cannot join the shared
        // chain; MySQL needs its own arm or the value truncates or nulls.
        assert_eq!(column(columns, "big"), ["18446744073709551615", ""]);
    }

    #[tokio::test]
    async fn a_null_row_is_still_null() {
        let Some(columns) = decoded().await else {
            return;
        };

        for name in ["dt", "ts", "d", "amount", "big", "t"] {
            assert_eq!(column(columns, name)[1], "", "{name} lost its NULL");
        }
    }
}
