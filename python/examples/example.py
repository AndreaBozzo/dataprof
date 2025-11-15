#!/usr/bin/env python3
"""
DataProfiler Python usage examples
"""

import dataprof
import pandas as pd
import time

def basic_usage():
    """Basic CSV analysis example"""
    print("🔍 Basic CSV Analysis")
    print("=" * 30)

    # Analyze single CSV file
    profiles = dataprof.analyze_csv_file("data.csv")

    for profile in profiles:
        print(f"📊 {profile.name}:")
        print(f"   Type: {profile.data_type}")
        print(f"   Rows: {profile.total_count}")
        print(f"   Nulls: {profile.null_percentage:.1f}%")
        print(f"   Unique: {profile.uniqueness_ratio:.2f}")
        print()

def quality_assessment():
    """Data quality assessment example"""
    print("🔍 Quality Assessment")
    print("=" * 30)

    # Comprehensive quality check
    report = dataprof.analyze_csv_with_quality("data.csv")

    print(f"📈 Overall Quality Score: {report.quality_score():.1f}%")
    print(f"📊 Dataset: {report.total_rows} rows × {report.total_columns} columns")
    print(f"⚡ Scan time: {report.scan_time_ms}ms")

    # Display data quality metrics details
    metrics = report.data_quality_metrics
    print(f"\n📊 Quality Metrics Breakdown:")
    print(f"  📋 Completeness: {metrics.completeness_summary()}")
    print(f"  🔧 Consistency: {metrics.consistency_summary()}")
    print(f"  🔑 Uniqueness: {metrics.uniqueness_summary()}")
    print(f"  🎯 Accuracy: {metrics.accuracy_summary()}")
    print(f"  ⏱️ Timeliness: {metrics.timeliness_summary()}")

    # Check for quality issues
    if metrics.missing_values_ratio > 10.0 or metrics.duplicate_rows > 0:
        print("\n⚠️ Quality Issues Detected:")
        if metrics.missing_values_ratio > 10.0:
            print(f"  • High missing values: {metrics.missing_values_ratio:.1f}%")
        if metrics.duplicate_rows > 0:
            print(f"  • Duplicate rows found: {metrics.duplicate_rows}")
        if metrics.format_violations > 0:
            print(f"  • Format violations: {metrics.format_violations}")
    else:
        print("\n✅ No major quality issues detected!")

def batch_processing():
    """Batch processing example"""
    print("🔍 Batch Processing")
    print("=" * 30)

    # Process entire directory
    result = dataprof.batch_analyze_directory(
        "/data/warehouse",
        recursive=True,
        parallel=True,
        max_concurrent=8
    )

    print(f"📊 Processed {result.processed_files} files")
    print(f"⏱️ Duration: {result.total_duration_secs:.2f} seconds")
    print(f"📈 Average Quality: {result.average_quality_score:.1f}%")
    print(f"❌ Failed: {result.failed_files}")

    # Process with glob pattern
    result = dataprof.batch_analyze_glob(
        "/data/**/*_staging_*.csv",
        parallel=True
    )

    print(f"📂 Staging files processed: {result.processed_files}")

def airflow_integration():
    """Example Airflow DAG task"""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime, timedelta

    def quality_check_task(**context):
        """Data quality check task"""
        file_path = context['params']['file_path']
        threshold = context['params'].get('quality_threshold', 80.0)

        # Run quality assessment
        report = dataprof.analyze_csv_with_quality(file_path)
        score = report.quality_score()

        # Log results
        print(f"Quality score: {score:.1f}% (threshold: {threshold}%)")

        if score < threshold:
            # Fail the task if quality is too low
            metrics = report.data_quality_metrics

            error_msg = f"Data quality below threshold ({score:.1f}% < {threshold}%)\n"
            error_msg += f"Completeness: {metrics.complete_records_ratio:.1f}%\n"
            error_msg += f"Consistency: {metrics.data_type_consistency:.1f}%\n"
            error_msg += f"Missing values: {metrics.missing_values_ratio:.1f}%\n"
            if metrics.duplicate_rows > 0:
                error_msg += f"Duplicate rows: {metrics.duplicate_rows}\n"

            raise ValueError(error_msg)

        return {
            'quality_score': score,
            'rows_scanned': report.rows_scanned,
            'scan_time_ms': report.scan_time_ms,
        }

    # DAG definition
    dag = DAG(
        'data_quality_check',
        default_args={
            'owner': 'data-team',
            'depends_on_past': False,
            'start_date': datetime(2024, 1, 1),
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(hours=6),
        catchup=False,
    )

    # Quality check task
    quality_check = PythonOperator(
        task_id='quality_check',
        python_callable=quality_check_task,
        params={
            'file_path': '/data/daily_export.csv',
            'quality_threshold': 85.0
        },
        dag=dag,
    )

def pandas_comparison():
    """Performance comparison with pandas"""
    print("🔍 Performance vs Pandas")
    print("=" * 30)

    file_path = "large_dataset.csv"

    # DataProfiler (Rust-powered)
    start_time = time.time()
    profiles = dataprof.analyze_csv_file(file_path)
    dataprof_time = time.time() - start_time

    print(f"⚡ DataProfiler: {dataprof_time:.2f}s")

    # Pandas equivalent
    start_time = time.time()
    df = pd.read_csv(file_path)
    df_info = df.info()
    df_describe = df.describe()
    df_nulls = df.isnull().sum()
    pandas_time = time.time() - start_time

    print(f"🐼 Pandas: {pandas_time:.2f}s")
    print(f"📊 Speedup: {pandas_time / dataprof_time:.1f}x faster")

if __name__ == "__main__":
    print("🚀 DataProfiler Python Examples")
    print("=" * 50)

    # Run examples (commented out as they require actual data files)
    # basic_usage()
    # quality_assessment()
    # batch_processing()
    # pandas_comparison()

    print("💡 See function definitions for usage examples!")
