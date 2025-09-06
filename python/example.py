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

    if report.issues:
        print(f"\n⚠️ Quality Issues Found ({len(report.issues)}):")

        # Group by severity
        high_issues = report.issues_by_severity("high")
        medium_issues = report.issues_by_severity("medium")
        low_issues = report.issues_by_severity("low")

        if high_issues:
            print(f"🔴 Critical ({len(high_issues)} issues)")
            for issue in high_issues:
                print(f"   • {issue.description}")

        if medium_issues:
            print(f"🟡 Medium ({len(medium_issues)} issues)")
            for issue in medium_issues:
                print(f"   • {issue.description}")

        if low_issues:
            print(f"🔵 Low ({len(low_issues)} issues)")
            for issue in low_issues:
                print(f"   • {issue.description}")
    else:
        print("✅ No quality issues detected!")

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
    print(f"⚡ Speed: {result.files_per_second:.1f} files/second")
    print(f"📈 Average Quality: {result.average_quality_score:.1f}%")
    print(f"⚠️ Total Issues: {result.total_quality_issues}")

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
            high_issues = report.issues_by_severity("high")
            medium_issues = report.issues_by_severity("medium")

            error_msg = f"Data quality below threshold ({score:.1f}% < {threshold}%)\n"
            if high_issues:
                error_msg += f"Critical issues: {len(high_issues)}\n"
            if medium_issues:
                error_msg += f"Medium issues: {len(medium_issues)}\n"

            raise ValueError(error_msg)

        return {
            'quality_score': score,
            'total_issues': len(report.issues),
            'file_size_mb': report.scan_time_ms / 1000,  # Approximate from scan time
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
