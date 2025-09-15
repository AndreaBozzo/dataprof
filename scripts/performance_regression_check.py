#!/usr/bin/env python3
"""
Performance regression detection for DataProfiler CI.
Compares current benchmark results against historical baselines.
"""

import json
import sys
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import argparse

class RegressionDetector:
    def __init__(self, baseline_file: Optional[Path] = None):
        self.baseline_file = baseline_file or Path('benchmark-results/performance_baseline.json')
        self.thresholds = {
            'speed_regression_threshold': 1.5,  # 50% slower is a regression
            'memory_regression_threshold': 2.0,  # 100% more memory is a regression
            'minimum_samples': 3,  # Need at least 3 historical samples
        }

    def load_baseline(self) -> Dict:
        """Load historical baseline performance data"""
        if not self.baseline_file.exists():
            print(f"⚠️  No baseline file found at {self.baseline_file}")
            return {'benchmarks': []}

        try:
            with open(self.baseline_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ Error loading baseline: {e}")
            return {'benchmarks': []}

    def save_baseline(self, data: Dict) -> None:
        """Save updated baseline data"""
        try:
            with open(self.baseline_file, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"✅ Baseline updated at {self.baseline_file}")
        except Exception as e:
            print(f"❌ Error saving baseline: {e}")

    def add_benchmark_result(self, results: List[Dict]) -> None:
        """Add new benchmark results to baseline history"""
        baseline_data = self.load_baseline()

        # Add current results with timestamp
        import time
        current_entry = {
            'timestamp': time.time(),
            'commit': os.getenv('GITHUB_SHA', 'local'),
            'branch': os.getenv('GITHUB_REF_NAME', 'local'),
            'results': results
        }

        baseline_data.setdefault('benchmarks', []).append(current_entry)

        # Keep only last 10 benchmark runs to avoid file bloat
        baseline_data['benchmarks'] = baseline_data['benchmarks'][-10:]

        self.save_baseline(baseline_data)

    def calculate_performance_metrics(self, results: List[Dict]) -> Dict[str, Dict]:
        """Extract key performance metrics from benchmark results"""
        metrics = {}

        for benchmark in results:
            size_mb = benchmark['file_size_mb']

            # Find DataProfiler results
            dataprof_result = None
            for result in benchmark['results']:
                if result['tool'] == 'DataProfiler' and result['success']:
                    dataprof_result = result
                    break

            if dataprof_result:
                metrics[f"{size_mb}MB"] = {
                    'time_seconds': dataprof_result['time_seconds'],
                    'memory_mb': dataprof_result['memory_mb'],
                    'rows_processed': dataprof_result['rows_processed']
                }

        return metrics

    def check_regression(self, current_results: List[Dict]) -> Tuple[bool, List[str]]:
        """Check if current results show performance regression"""
        baseline_data = self.load_baseline()

        if len(baseline_data.get('benchmarks', [])) < self.thresholds['minimum_samples']:
            print(f"ℹ️  Insufficient historical data ({len(baseline_data.get('benchmarks', []))} samples)")
            print("   Need at least 3 samples for regression detection")
            return False, ["Insufficient historical data for regression analysis"]

        current_metrics = self.calculate_performance_metrics(current_results)
        issues = []

        # Calculate historical averages
        historical_metrics = {}
        for benchmark_entry in baseline_data['benchmarks']:
            entry_metrics = self.calculate_performance_metrics(benchmark_entry['results'])
            for size, metrics in entry_metrics.items():
                if size not in historical_metrics:
                    historical_metrics[size] = {'time': [], 'memory': []}
                historical_metrics[size]['time'].append(metrics['time_seconds'])
                historical_metrics[size]['memory'].append(metrics['memory_mb'])

        # Calculate averages
        avg_metrics = {}
        for size, data in historical_metrics.items():
            if len(data['time']) >= 2:  # Need at least 2 points for comparison
                avg_metrics[size] = {
                    'avg_time': sum(data['time']) / len(data['time']),
                    'avg_memory': sum(data['memory']) / len(data['memory'])
                }

        # Check for regressions
        has_regression = False

        for size, current in current_metrics.items():
            if size not in avg_metrics:
                continue

            avg = avg_metrics[size]

            # Check speed regression
            speed_ratio = current['time_seconds'] / avg['avg_time']
            if speed_ratio > self.thresholds['speed_regression_threshold']:
                has_regression = True
                issues.append(f"⚠️  Speed regression detected for {size}: {speed_ratio:.1f}x slower than baseline")

            # Check memory regression
            memory_ratio = current['memory_mb'] / avg['avg_memory']
            if memory_ratio > self.thresholds['memory_regression_threshold']:
                has_regression = True
                issues.append(f"⚠️  Memory regression detected for {size}: {memory_ratio:.1f}x more memory than baseline")

        return has_regression, issues

    def generate_performance_report(self, current_results: List[Dict]) -> str:
        """Generate a detailed performance report"""
        report = []
        report.append("# 📊 Performance Analysis Report")
        report.append("")

        # Current performance summary
        report.append("## 🎯 Current Performance")
        current_metrics = self.calculate_performance_metrics(current_results)

        for size, metrics in current_metrics.items():
            report.append(f"- **{size}**: {metrics['time_seconds']:.2f}s, {metrics['memory_mb']:.1f}MB, {metrics['rows_processed']:,} rows")

        # Regression analysis
        has_regression, issues = self.check_regression(current_results)

        report.append("")
        if has_regression:
            report.append("## ⚠️  Regression Analysis")
            for issue in issues:
                report.append(f"- {issue}")
        else:
            report.append("## ✅ Regression Analysis")
            report.append("- No performance regressions detected")

        # Performance claims validation
        report.append("")
        report.append("## 🏆 Performance Claims Validation")

        for benchmark in current_results:
            size_mb = benchmark['file_size_mb']
            dataprof_result = None
            pandas_result = None

            for result in benchmark['results']:
                if result['tool'] == 'DataProfiler' and result['success']:
                    dataprof_result = result
                elif result['tool'] == 'pandas' and result['success']:
                    pandas_result = result

            if dataprof_result and pandas_result:
                speedup = pandas_result['time_seconds'] / dataprof_result['time_seconds']
                memory_ratio = pandas_result['memory_mb'] / dataprof_result['memory_mb']

                status = "✅" if speedup >= 5.0 else "⚠️"
                report.append(f"- {status} **{size_mb}MB**: {speedup:.1f}x faster, {memory_ratio:.1f}x memory efficient vs pandas")

        return "\n".join(report)

def main():
    parser = argparse.ArgumentParser(description='Performance regression detection')
    parser.add_argument('results_file', help='JSON file with benchmark results')
    parser.add_argument('--baseline', help='Baseline file path', default='benchmark-results/performance_baseline.json')
    parser.add_argument('--update-baseline', action='store_true', help='Update baseline with current results')
    parser.add_argument('--fail-on-regression', action='store_true', help='Exit with error code on regression')

    args = parser.parse_args()

    detector = RegressionDetector(Path(args.baseline))

    # Load current results
    try:
        with open(args.results_file, 'r') as f:
            current_results = json.load(f)
    except Exception as e:
        print(f"❌ Error loading results file: {e}")
        sys.exit(1)

    # Check for regressions
    has_regression, issues = detector.check_regression(current_results)

    # Generate report
    report = detector.generate_performance_report(current_results)
    print(report)

    # Update baseline if requested
    if args.update_baseline:
        detector.add_benchmark_result(current_results)

    # Exit with error if regression detected and flag is set
    if has_regression and args.fail_on_regression:
        print(f"\n❌ Performance regression detected!")
        for issue in issues:
            print(f"   {issue}")
        sys.exit(1)
    elif has_regression:
        print(f"\n⚠️  Performance regression detected (not failing due to flag)")
        for issue in issues:
            print(f"   {issue}")
    else:
        print(f"\n✅ No performance regressions detected")

    sys.exit(0)

if __name__ == '__main__':
    main()