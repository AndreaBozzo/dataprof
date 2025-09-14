#!/usr/bin/env python3
"""
External tool comparison script for validating DataProfiler's "10x faster" performance claims.
Compares DataProfiler against pandas, Great Expectations, and other tools.
"""

import pandas as pd
import numpy as np
import time
import subprocess
import json
import sys
import os
import tempfile
import psutil
from pathlib import Path
from typing import Dict, List, Tuple, Any
import csv

def generate_test_csv(filepath: Path, size_mb: int) -> Path:
    """Generate test CSV matching the large_scale_benchmarks.rs format"""
    print(f"Generating {size_mb}MB test CSV...")

    # Calculate approximate rows needed
    avg_row_size = 120
    target_bytes = size_mb * 1024 * 1024
    rows_needed = target_bytes // avg_row_size

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'email', 'age', 'salary', 'created_at', 'active', 'score', 'category', 'description'])

        for i in range(rows_needed):
            writer.writerow([
                i,
                f'User_{i}',
                f'user{i}@example.com',
                18 + (i % 65),
                30000 + (i % 100000),
                f'2023-{1 + (i % 12):02d}-{1 + (i % 28):02d}',
                i % 3 == 0,
                round((i * 1.7) % 100, 2),
                ['Premium', 'Standard', 'Basic', 'Enterprise', 'Trial'][i % 5],
                f'Description for user {i} with extended content and metadata'
            ])

            if i % 50000 == 0 and i > 0:
                print(f"Generated {i} rows...")

    actual_size = filepath.stat().st_size / (1024 * 1024)
    print(f"Generated file: {actual_size:.1f}MB")
    return filepath

class BenchmarkRunner:
    def __init__(self):
        self.results = {}

    def measure_memory_and_time(self, func, *args, **kwargs) -> Tuple[float, float, Any]:
        """Measure execution time, peak memory usage, and return result"""
        process = psutil.Process()

        # Get initial memory
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        # Get peak memory (approximate)
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        peak_memory = max(final_memory, initial_memory)

        return end_time - start_time, peak_memory - initial_memory, result

    def benchmark_dataprof(self, csv_path: Path) -> Dict[str, Any]:
        """Benchmark DataProfiler CLI"""
        print("Benchmarking DataProfiler...")

        def run_dataprof():
            result = subprocess.run([
                'cargo', 'run', '--release', '--',
                str(csv_path), '--format', 'json'
            ], capture_output=True, text=True, cwd='.')

            if result.returncode != 0:
                raise Exception(f"DataProfiler failed: {result.stderr}")

            return json.loads(result.stdout)

        time_taken, memory_used, result = self.measure_memory_and_time(run_dataprof)

        return {
            'tool': 'DataProfiler',
            'time_seconds': time_taken,
            'memory_mb': memory_used,
            'rows_processed': result.get('rows', 0),
            'columns_processed': result.get('columns', 0),
            'success': True
        }

    def benchmark_pandas(self, csv_path: Path) -> Dict[str, Any]:
        """Benchmark pandas.describe() equivalent"""
        print("Benchmarking pandas...")

        def run_pandas():
            # Read and perform basic profiling similar to DataProfiler
            df = pd.read_csv(csv_path)

            # Basic statistics (similar to DataProfiler's analysis)
            numeric_stats = df.describe()
            object_stats = df.select_dtypes(include=['object']).describe()
            null_counts = df.isnull().sum()
            dtypes = df.dtypes

            return {
                'rows': len(df),
                'columns': len(df.columns),
                'numeric_stats': numeric_stats.to_dict(),
                'object_stats': object_stats.to_dict() if not object_stats.empty else {},
                'null_counts': null_counts.to_dict(),
                'dtypes': dtypes.to_dict()
            }

        try:
            time_taken, memory_used, result = self.measure_memory_and_time(run_pandas)

            return {
                'tool': 'pandas',
                'time_seconds': time_taken,
                'memory_mb': memory_used,
                'rows_processed': result['rows'],
                'columns_processed': result['columns'],
                'success': True
            }
        except Exception as e:
            return {
                'tool': 'pandas',
                'time_seconds': float('inf'),
                'memory_mb': float('inf'),
                'rows_processed': 0,
                'columns_processed': 0,
                'success': False,
                'error': str(e)
            }

    def benchmark_polars(self, csv_path: Path) -> Dict[str, Any]:
        """Benchmark Polars (if available)"""
        try:
            import polars as pl
            print("Benchmarking Polars...")

            def run_polars():
                df = pl.read_csv(csv_path)

                # Basic profiling similar to DataProfiler
                stats = df.describe()
                null_counts = df.null_count()
                dtypes = df.dtypes

                return {
                    'rows': df.height,
                    'columns': df.width,
                    'stats': stats.to_dict(),
                    'null_counts': null_counts.to_dict() if hasattr(null_counts, 'to_dict') else {},
                    'dtypes': [str(dt) for dt in dtypes]
                }

            time_taken, memory_used, result = self.measure_memory_and_time(run_polars)

            return {
                'tool': 'polars',
                'time_seconds': time_taken,
                'memory_mb': memory_used,
                'rows_processed': result['rows'],
                'columns_processed': result['columns'],
                'success': True
            }
        except ImportError:
            print("Polars not available, skipping...")
            return {
                'tool': 'polars',
                'success': False,
                'error': 'Not installed'
            }
        except Exception as e:
            return {
                'tool': 'polars',
                'success': False,
                'error': str(e)
            }

def run_comparison_benchmark(size_mb: int) -> Dict[str, Any]:
    """Run comparison benchmark for given file size"""

    with tempfile.TemporaryDirectory() as temp_dir:
        csv_path = Path(temp_dir) / f'test_{size_mb}mb.csv'
        generate_test_csv(csv_path, size_mb)

        runner = BenchmarkRunner()
        results = []

        # Benchmark each tool
        tools = [
            ('DataProfiler', runner.benchmark_dataprof),
            ('pandas', runner.benchmark_pandas),
            ('polars', runner.benchmark_polars),
        ]

        for tool_name, benchmark_func in tools:
            print(f"\n--- Benchmarking {tool_name} on {size_mb}MB file ---")
            try:
                result = benchmark_func(csv_path)
                results.append(result)

                if result['success']:
                    print(f"✅ {tool_name}: {result['time_seconds']:.2f}s, {result['memory_mb']:.1f}MB memory")
                else:
                    print(f"❌ {tool_name}: {result.get('error', 'Unknown error')}")

            except Exception as e:
                print(f"❌ {tool_name} crashed: {e}")
                results.append({
                    'tool': tool_name,
                    'success': False,
                    'error': str(e)
                })

        return {
            'file_size_mb': size_mb,
            'results': results,
            'timestamp': time.time()
        }

def analyze_performance_claims(results: List[Dict[str, Any]]) -> None:
    """Analyze results and validate performance claims"""
    print("\n" + "="*60)
    print("PERFORMANCE CLAIMS VALIDATION")
    print("="*60)

    for benchmark in results:
        size_mb = benchmark['file_size_mb']
        print(f"\n📊 {size_mb}MB File Results:")
        print("-" * 40)

        successful_results = [r for r in benchmark['results'] if r['success']]

        if not successful_results:
            print("❌ No successful benchmarks for this file size")
            continue

        # Find DataProfiler and pandas results
        dataprof_result = next((r for r in successful_results if r['tool'] == 'DataProfiler'), None)
        pandas_result = next((r for r in successful_results if r['tool'] == 'pandas'), None)

        for result in successful_results:
            tool = result['tool']
            time_s = result['time_seconds']
            memory_mb = result['memory_mb']
            rows = result['rows_processed']

            print(f"{tool:12}: {time_s:6.2f}s | {memory_mb:6.1f}MB | {rows:,} rows")

        # Validate "10x faster" claim
        if dataprof_result and pandas_result:
            speedup = pandas_result['time_seconds'] / dataprof_result['time_seconds']
            memory_ratio = pandas_result['memory_mb'] / dataprof_result['memory_mb']

            print(f"\n🚀 PERFORMANCE COMPARISON:")
            print(f"   Speed improvement: {speedup:.1f}x faster than pandas")
            print(f"   Memory efficiency: {memory_ratio:.1f}x less memory than pandas")

            if speedup >= 10.0:
                print("✅ CLAIM VALIDATED: DataProfiler is ≥10x faster than pandas")
            elif speedup >= 5.0:
                print("⚠️  CLAIM PARTIAL: DataProfiler is 5-10x faster than pandas")
            else:
                print("❌ CLAIM FAILED: DataProfiler is <5x faster than pandas")

def main():
    if len(sys.argv) > 1:
        sizes = [int(x) for x in sys.argv[1:]]
    else:
        sizes = [1, 10, 50]  # Default test sizes

    print("🚀 DataProfiler Performance Comparison")
    print("="*50)
    print(f"Testing file sizes: {sizes}MB")

    all_results = []

    for size_mb in sizes:
        print(f"\n{'='*20} {size_mb}MB TEST {'='*20}")
        result = run_comparison_benchmark(size_mb)
        all_results.append(result)

    # Analyze and report
    analyze_performance_claims(all_results)

    # Save results to JSON for CI
    results_file = Path('benchmark_comparison_results.json')
    with open(results_file, 'w') as f:
        json.dump(all_results, f, indent=2)

    print(f"\n📁 Results saved to: {results_file}")

    # Exit with error code if any claims failed
    for benchmark in all_results:
        successful_results = [r for r in benchmark['results'] if r['success']]
        dataprof_result = next((r for r in successful_results if r['tool'] == 'DataProfiler'), None)
        pandas_result = next((r for r in successful_results if r['tool'] == 'pandas'), None)

        if dataprof_result and pandas_result:
            speedup = pandas_result['time_seconds'] / dataprof_result['time_seconds']
            if speedup < 5.0:  # Minimum acceptable performance
                print("\n❌ Performance regression detected!")
                sys.exit(1)

    print("\n✅ All performance benchmarks passed!")

if __name__ == '__main__':
    main()
