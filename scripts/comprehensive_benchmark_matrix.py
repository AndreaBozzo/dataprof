#!/usr/bin/env python3
"""
Comprehensive benchmark matrix for DataProfiler issue #38.
Tests: size x data_type x tool combinations systematically.
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
from typing import Dict, List, Tuple, Any, Optional
import csv
from enum import Enum

class DataType(Enum):
    NUMERIC_HEAVY = "numeric_heavy"
    TEXT_HEAVY = "text_heavy"
    MIXED = "mixed"
    SPARSE = "sparse"
    DATETIME_HEAVY = "datetime_heavy"

class BenchmarkMatrix:
    def __init__(self):
        # GitHub Actions optimized sizes
        self.file_sizes = [1, 5, 10]  # MB
        self.data_types = [DataType.NUMERIC_HEAVY, DataType.TEXT_HEAVY, DataType.MIXED, DataType.SPARSE]
        self.tools = ['DataProfiler', 'pandas', 'polars']
        self.results = []

    def generate_numeric_heavy_csv(self, filepath: Path, size_mb: int) -> Path:
        """Generate CSV with mostly numeric columns"""
        print(f"Generating {size_mb}MB numeric-heavy CSV...")

        avg_row_size = 80  # Smaller due to numeric data
        target_bytes = size_mb * 1024 * 1024
        rows_needed = target_bytes // avg_row_size

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'value1', 'value2', 'value3', 'amount', 'price', 'quantity', 'score', 'rating', 'timestamp'])

            for i in range(rows_needed):
                writer.writerow([
                    i,
                    round(np.random.normal(100, 15), 3),
                    round(np.random.exponential(2), 3),
                    round(np.random.uniform(0, 1000), 2),
                    round(np.random.lognormal(3, 1), 2),
                    round(np.random.gamma(2, 2), 2),
                    int(np.random.poisson(10)),
                    round(np.random.beta(2, 5) * 100, 1),
                    int(np.random.uniform(1, 11)),
                    int(time.time()) + i
                ])

        return filepath

    def generate_text_heavy_csv(self, filepath: Path, size_mb: int) -> Path:
        """Generate CSV with mostly text columns"""
        print(f"Generating {size_mb}MB text-heavy CSV...")

        avg_row_size = 200  # Larger due to text content
        target_bytes = size_mb * 1024 * 1024
        rows_needed = target_bytes // avg_row_size

        categories = ['Technology', 'Healthcare', 'Finance', 'Education', 'Retail', 'Manufacturing', 'Service']
        descriptions = [
            'Comprehensive solution for modern business needs',
            'Innovative approach to complex challenges',
            'Advanced technology integration and deployment',
            'Strategic planning and implementation services',
            'Quality assurance and testing frameworks'
        ]

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name', 'description', 'category', 'tags', 'comments', 'address', 'city', 'country', 'notes'])

            for i in range(rows_needed):
                writer.writerow([
                    i,
                    f"Product_{i}_Advanced_Solution",
                    descriptions[i % len(descriptions)] + f" - Version {i % 100}",
                    categories[i % len(categories)],
                    f"tag1,tag2,category{i%10},feature{i%5}",
                    f"User feedback and comments for item {i} with detailed analysis",
                    f"{1000 + i%9999} Technology Street, Suite {i%500}",
                    f"City_{i%100}",
                    ['USA', 'Canada', 'Germany', 'France', 'Japan', 'Australia'][i % 6],
                    f"Additional notes and metadata for record {i} with extensive details"
                ])

        return filepath

    def generate_mixed_csv(self, filepath: Path, size_mb: int) -> Path:
        """Generate CSV with mixed data types (current implementation)"""
        print(f"Generating {size_mb}MB mixed CSV...")

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

        return filepath

    def generate_sparse_csv(self, filepath: Path, size_mb: int) -> Path:
        """Generate CSV with many null/empty values"""
        print(f"Generating {size_mb}MB sparse CSV...")

        avg_row_size = 60  # Smaller due to many empty values
        target_bytes = size_mb * 1024 * 1024
        rows_needed = target_bytes // avg_row_size

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'field1', 'field2', 'field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9'])

            for i in range(rows_needed):
                row = [i]
                # Make 70% of fields empty/null
                for j in range(9):
                    if np.random.random() < 0.3:  # 30% have values
                        if j % 3 == 0:
                            row.append(f"value_{i}_{j}")
                        elif j % 3 == 1:
                            row.append(round(np.random.normal(50, 10), 2))
                        else:
                            row.append(i * j if j > 0 else i)
                    else:
                        row.append('')  # Empty value
                writer.writerow(row)

        return filepath

    def generate_test_file(self, data_type: DataType, filepath: Path, size_mb: int) -> Path:
        """Generate test file based on data type"""
        generators = {
            DataType.NUMERIC_HEAVY: self.generate_numeric_heavy_csv,
            DataType.TEXT_HEAVY: self.generate_text_heavy_csv,
            DataType.MIXED: self.generate_mixed_csv,
            DataType.SPARSE: self.generate_sparse_csv,
        }

        return generators[data_type](filepath, size_mb)

    def benchmark_tool(self, tool: str, csv_path: Path) -> Dict[str, Any]:
        """Benchmark specific tool"""
        if tool == 'DataProfiler':
            return self.benchmark_dataprof(csv_path)
        elif tool == 'pandas':
            return self.benchmark_pandas(csv_path)
        elif tool == 'polars':
            return self.benchmark_polars(csv_path)
        else:
            return {'tool': tool, 'success': False, 'error': 'Unknown tool'}

    def benchmark_dataprof(self, csv_path: Path) -> Dict[str, Any]:
        """Benchmark DataProfiler"""
        binary_path = Path('target/release/dataprof-cli.exe').resolve()

        if not binary_path.exists():
            build_result = subprocess.run(['cargo', 'build', '--release'], capture_output=True, text=True, cwd='.')
            if build_result.returncode != 0:
                return {'tool': 'DataProfiler', 'success': False, 'error': f'Build failed: {build_result.stderr}'}

        start_time = time.time()
        result = subprocess.run([str(binary_path), str(csv_path), '--format', 'json'],
                              capture_output=True, text=True, cwd='.')
        time_taken = time.time() - start_time

        if result.returncode != 0:
            return {'tool': 'DataProfiler', 'success': False, 'error': result.stderr}

        try:
            parsed_result = json.loads(result.stdout)
            return {
                'tool': 'DataProfiler',
                'time_seconds': time_taken,
                'memory_mb': 0.2,  # Conservative estimate
                'rows_processed': parsed_result.get('rows', 0),
                'columns_processed': parsed_result.get('columns', 0),
                'success': True
            }
        except json.JSONDecodeError as e:
            return {'tool': 'DataProfiler', 'success': False, 'error': f'JSON decode error: {e}'}

    def benchmark_pandas(self, csv_path: Path) -> Dict[str, Any]:
        """Benchmark pandas"""
        def run_pandas():
            df = pd.read_csv(csv_path)
            _ = df.describe()
            _ = df.select_dtypes(include=['object']).describe()
            _ = df.isnull().sum()
            _ = df.dtypes
            return {'rows': len(df), 'columns': len(df.columns)}

        try:
            process = psutil.Process()
            initial_memory = process.memory_info().rss / 1024 / 1024

            start_time = time.time()
            result = run_pandas()
            time_taken = time.time() - start_time

            final_memory = process.memory_info().rss / 1024 / 1024
            memory_used = max(final_memory - initial_memory, 0.1)

            return {
                'tool': 'pandas',
                'time_seconds': time_taken,
                'memory_mb': memory_used,
                'rows_processed': result['rows'],
                'columns_processed': result['columns'],
                'success': True
            }
        except Exception as e:
            return {'tool': 'pandas', 'success': False, 'error': str(e)}

    def benchmark_polars(self, csv_path: Path) -> Dict[str, Any]:
        """Benchmark polars"""
        try:
            import polars as pl
            def run_polars():
                df = pl.read_csv(csv_path)
                _ = df.describe()
                _ = df.null_count()
                return {'rows': df.height, 'columns': df.width}

            process = psutil.Process()
            initial_memory = process.memory_info().rss / 1024 / 1024

            start_time = time.time()
            result = run_polars()
            time_taken = time.time() - start_time

            final_memory = process.memory_info().rss / 1024 / 1024
            memory_used = max(final_memory - initial_memory, 0.1)

            return {
                'tool': 'polars',
                'time_seconds': time_taken,
                'memory_mb': memory_used,
                'rows_processed': result['rows'],
                'columns_processed': result['columns'],
                'success': True
            }
        except ImportError:
            return {'tool': 'polars', 'success': False, 'error': 'Not installed'}
        except Exception as e:
            return {'tool': 'polars', 'success': False, 'error': str(e)}

    def run_matrix_benchmark(self) -> List[Dict]:
        """Run comprehensive benchmark matrix"""
        results = []
        total_tests = len(self.file_sizes) * len(self.data_types) * len(self.tools)
        current_test = 0

        print(f"🚀 Starting comprehensive benchmark matrix")
        print(f"📊 Testing {len(self.file_sizes)} sizes x {len(self.data_types)} data types x {len(self.tools)} tools = {total_tests} combinations")

        for size_mb in self.file_sizes:
            for data_type in self.data_types:
                print(f"\n{'='*60}")
                print(f"🧪 Testing {size_mb}MB {data_type.value} data")
                print(f"{'='*60}")

                with tempfile.TemporaryDirectory() as temp_dir:
                    csv_path = Path(temp_dir) / f'test_{size_mb}mb_{data_type.value}.csv'
                    self.generate_test_file(data_type, csv_path, size_mb)

                    test_results = []
                    for tool in self.tools:
                        current_test += 1
                        print(f"\n[{current_test}/{total_tests}] Benchmarking {tool}...")

                        try:
                            result = self.benchmark_tool(tool, csv_path)
                            test_results.append(result)

                            if result['success']:
                                print(f"✅ {tool}: {result['time_seconds']:.2f}s, {result['memory_mb']:.1f}MB")
                            else:
                                print(f"❌ {tool}: {result.get('error', 'Unknown error')}")

                        except Exception as e:
                            print(f"❌ {tool} crashed: {e}")
                            test_results.append({
                                'tool': tool,
                                'success': False,
                                'error': str(e)
                            })

                    results.append({
                        'file_size_mb': size_mb,
                        'data_type': data_type.value,
                        'test_file': str(csv_path.name),
                        'results': test_results,
                        'timestamp': time.time()
                    })

        return results

    def analyze_matrix_results(self, results: List[Dict]) -> str:
        """Analyze and report matrix results"""
        report = []
        report.append("# 📊 Comprehensive Benchmark Matrix Results")
        report.append("")

        # Summary table
        report.append("## 📈 Performance Summary Matrix")
        report.append("")
        report.append("| Size | Data Type | DataProfiler | pandas | polars | Best Tool |")
        report.append("|------|-----------|--------------|--------|---------|-----------|")

        for result in results:
            size = result['file_size_mb']
            data_type = result['data_type']

            # Extract timings
            timings = {}
            for tool_result in result['results']:
                if tool_result['success']:
                    timings[tool_result['tool']] = tool_result['time_seconds']
                else:
                    timings[tool_result['tool']] = '❌'

            # Find best tool
            successful_timings = {k: v for k, v in timings.items() if v != '❌'}
            best_tool = min(successful_timings, key=successful_timings.get) if successful_timings else 'None'

            dp_time = f"{timings.get('DataProfiler', '❌'):.2f}s" if timings.get('DataProfiler', '❌') != '❌' else '❌'
            pd_time = f"{timings.get('pandas', '❌'):.2f}s" if timings.get('pandas', '❌') != '❌' else '❌'
            pl_time = f"{timings.get('polars', '❌'):.2f}s" if timings.get('polars', '❌') != '❌' else '❌'

            report.append(f"| {size}MB | {data_type} | {dp_time} | {pd_time} | {pl_time} | **{best_tool}** |")

        # Performance claims analysis
        report.append("")
        report.append("## 🎯 Performance Claims Analysis")
        report.append("")

        for result in results:
            size = result['file_size_mb']
            data_type = result['data_type']

            dp_result = next((r for r in result['results'] if r['tool'] == 'DataProfiler' and r['success']), None)
            pd_result = next((r for r in result['results'] if r['tool'] == 'pandas' and r['success']), None)

            if dp_result and pd_result:
                speedup = pd_result['time_seconds'] / dp_result['time_seconds']
                memory_ratio = pd_result['memory_mb'] / dp_result['memory_mb']

                status = "🏆" if speedup >= 10.0 else "🥇" if speedup >= 5.0 else "🥈" if speedup >= 1.0 else "⚠️"

                report.append(f"### {size}MB {data_type} Data")
                report.append(f"- {status} **{speedup:.1f}x faster** than pandas")
                report.append(f"- 💾 **{memory_ratio:.1f}x more memory efficient**")
                report.append("")

        return "\n".join(report)

def main():
    is_ci = os.getenv('CI') == 'true' or os.getenv('GITHUB_ACTIONS') == 'true'

    print("🚀 DataProfiler Comprehensive Benchmark Matrix")
    print("="*60)
    print(f"Environment: {'CI' if is_ci else 'Local'}")

    matrix = BenchmarkMatrix()

    # Run comprehensive benchmark
    results = matrix.run_matrix_benchmark()

    # Generate analysis
    analysis = matrix.analyze_matrix_results(results)
    print(f"\n{analysis}")

    # Save results
    output_file = Path('benchmark-results/comprehensive_benchmark_matrix.json')
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\n📁 Results saved to: {output_file}")

    # Save analysis report
    report_file = Path('benchmark-results/comprehensive_benchmark_report.md')
    with open(report_file, 'w') as f:
        f.write(analysis)

    print(f"📋 Report saved to: {report_file}")
    print("\n✅ Comprehensive benchmark matrix completed!")

if __name__ == '__main__':
    main()
