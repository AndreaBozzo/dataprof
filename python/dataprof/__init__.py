"""DataProf Python bindings"""

from ._dataprof import *
from ._dataprof import __version__

# Core exports for data profiling
__all__ = [
    # Core analysis functions
    "analyze_csv_file",
    "analyze_csv_with_quality",
    "analyze_json_file",
    "calculate_data_quality_metrics",
    "batch_analyze_glob",
    "batch_analyze_directory",

    # Python logging integration
    "configure_logging",
    "get_logger",
    "log_info",
    "log_debug",
    "log_warning",
    "log_error",

    # Enhanced analysis with logging
    "analyze_csv_with_logging",

    # Core classes
    "PyColumnProfile",
    "PyQualityReport",
    "PyDataQualityMetrics",
    "PyBatchResult",

    # Context managers
    "PyBatchAnalyzer",
    "PyCsvProcessor",

    # High-level API
    "profile",
    "ProfileReport",
]

import json
import os
from ._dataprof import analyze_csv_with_quality, PyQualityReport

class ProfileReport:
    """
    High-level wrapper for DataProf reports with export capabilities.
    """
    def __init__(self, report: PyQualityReport):
        self.report = report

    def save(self, path: str):
        """
        Save the profile report to a file.
        Format is inferred from extension: .html or .json
        """
        if path.endswith('.html'):
            content = self._to_html()
            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)
        elif path.endswith('.json'):
            content = self._to_json()
            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)
        else:
            raise ValueError("Unsupported format. Please use .html or .json")
        return self

    def _to_json(self):
        # Basic JSON dump of the metrics
        data = {
            "file_path": self.report.file_path,
            "total_rows": self.report.total_rows,
            "total_columns": self.report.total_columns,
            "scan_time_ms": self.report.scan_time_ms,
            "quality_score": self.report.quality_score(),
            "columns": []
        }
        for col in self.report.column_profiles:
            data["columns"].append({
                "name": col.name,
                "type": col.data_type,
                "null_percentage": col.null_percentage
            })
        return json.dumps(data, indent=2)

    def _to_html(self):
        # Generate variable cards
        variables_html = ""
        for i, col in enumerate(self.report.column_profiles):
            unique_display = f"{col.unique_count:,}" if col.unique_count is not None else "N/A"
            null_color = "bg-green-500" if col.null_percentage == 0 else ("bg-yellow-500" if col.null_percentage < 5 else "bg-red-500")
            
            # Badge color based on type
            type_badge_color = "bg-gray-100 text-gray-800"
            if col.data_type.lower() in ['integer', 'float', 'number']:
                type_badge_color = "bg-blue-100 text-blue-800"
            elif col.data_type.lower() in ['string', 'text']:
                type_badge_color = "bg-green-100 text-green-800"
            elif col.data_type.lower() in ['date', 'datetime']:
                type_badge_color = "bg-purple-100 text-purple-800"
            
            variables_html += f"""
            <div class="variable-card bg-white rounded-lg border border-gray-200 p-6 hover:shadow-md transition-shadow duration-200" data-name="{col.name.lower()}">
                <div class="flex flex-col md:flex-row md:items-start md:justify-between gap-4">
                    <div class="flex-1">
                        <div class="flex items-center gap-3 mb-2">
                            <h3 class="text-lg font-bold text-gray-900 font-mono">{col.name}</h3>
                            <span class="px-2.5 py-0.5 rounded-full text-xs font-medium {type_badge_color} border border-opacity-20">{col.data_type}</span>
                        </div>
                        
                        <div class="grid grid-cols-2 sm:grid-cols-4 gap-4 mt-4">
                            <!-- Valid -->
                            <div class="flex flex-col">
                                <span class="text-xs text-gray-500 uppercase tracking-wider font-semibold">Count</span>
                                <span class="text-sm font-medium text-gray-900">{col.total_count:,}</span>
                            </div>
                            
                            <!-- Missing -->
                            <div class="flex flex-col">
                                <span class="text-xs text-gray-500 uppercase tracking-wider font-semibold">Missing</span>
                                <div class="flex items-baseline gap-1">
                                    <span class="text-sm font-medium {('text-red-600' if col.null_percentage > 0 else 'text-gray-900')}">
                                        {col.null_count:,}
                                    </span>
                                    <span class="text-xs text-gray-400">({col.null_percentage:.1f}%)</span>
                                </div>
                            </div>

                            <!-- Unique -->
                            <div class="flex flex-col">
                                <span class="text-xs text-gray-500 uppercase tracking-wider font-semibold">Distinct</span>
                                <span class="text-sm font-medium text-gray-900">{unique_display}</span>
                            </div>

                            <!-- Memory/Other (Placeholder) -->
                            <div class="flex flex-col">
                                <span class="text-xs text-gray-500 uppercase tracking-wider font-semibold">Uniqueness</span>
                                <span class="text-sm font-medium text-gray-900">{col.uniqueness_ratio * 100:.1f}%</span>
                            </div>
                        </div>
                    </div>

                    <!-- Mini Visualization Bars -->
                    <div class="w-full md:w-48 flex flex-col gap-3 pt-2">
                        <div class="w-full">
                            <div class="flex justify-between text-xs mb-1">
                                <span class="text-gray-500">Completeness</span>
                                <span class="text-gray-700 font-medium">{100 - col.null_percentage:.1f}%</span>
                            </div>
                            <div class="w-full bg-gray-100 rounded-full h-2 overflow-hidden">
                                <div class="{null_color} h-2 rounded-full" style="width: {100 - col.null_percentage}%"></div>
                            </div>
                        </div>
                        <div class="w-full">
                             <div class="flex justify-between text-xs mb-1">
                                <span class="text-gray-500">Uniqueness</span>
                                <span class="text-gray-700 font-medium">{col.uniqueness_ratio * 100:.1f}%</span>
                            </div>
                            <div class="w-full bg-gray-100 rounded-full h-2 overflow-hidden">
                                <div class="bg-blue-500 h-2 rounded-full" style="width: {col.uniqueness_ratio * 100}%"></div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            """

        # Current date for footer
        from datetime import datetime
        generated_date = datetime.now().strftime("%Y-%m-%d %H:%M")

        html = f"""
<!DOCTYPE html>
<html lang="en" class="scroll-smooth">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Analysis Report - {self.report.file_path}</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {{
            theme: {{
                extend: {{
                    colors: {{
                        slate: {{ 850: '#1e293b' }}
                    }}
                }}
            }}
        }}
    </script>
    <style>
        body {{ font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
        .sidebar {{ width: 280px; }}
        .content {{ margin-left: 280px; }}
        @media (max-width: 1024px) {{
            .sidebar {{ display: none; }}
            .content {{ margin-left: 0; }}
        }}
    </style>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
</head>
<body class="bg-slate-50 text-slate-900 antialiased">

    <!-- Mobile Header -->
    <div class="lg:hidden bg-slate-900 text-white p-4 flex items-center justify-between sticky top-0 z-50">
        <h1 class="font-bold text-xl">DataProf</h1>
        <span class="text-xs text-slate-400">v0.4</span>
    </div>

    <div class="flex min-h-screen">
        <!-- Sidebar -->
        <aside class="sidebar fixed inset-y-0 left-0 bg-slate-900 text-white z-40 overflow-y-auto hidden lg:block">
            <div class="p-6">
                <div class="flex item-center gap-3 mb-8">
                    <div class="w-8 h-8 rounded bg-gradient-to-br from-blue-400 to-indigo-600 flex items-center justify-center font-bold text-lg">D</div>
                    <span class="font-bold text-xl tracking-tight">DataProf</span>
                </div>
                
                <nav class="space-y-1">
                    <a href="#overview" class="flex items-center gap-3 px-3 py-2 text-slate-100 bg-slate-800 rounded-md transition-colors">
                        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"></path></svg>
                        Overview
                    </a>
                    <a href="#variables" class="flex items-center gap-3 px-3 py-2 text-slate-400 hover:text-white hover:bg-slate-800 rounded-md transition-colors">
                        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 10h16M4 14h16M4 18h16"></path></svg>
                        Variables ({len(self.report.column_profiles)})
                    </a>
                </nav>

                <div class="mt-8 pt-8 border-t border-slate-700">
                    <h3 class="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-4">Meta Info</h3>
                    <div class="space-y-3 text-sm text-slate-400">
                        <div class="flex justify-between">
                            <span>Rows</span>
                            <span class="text-white">{self.report.total_rows:,}</span>
                        </div>
                         <div class="flex justify-between">
                            <span>Columns</span>
                            <span class="text-white">{self.report.total_columns}</span>
                        </div>
                        <div class="flex justify-between">
                            <span>Analysis Time</span>
                            <span class="text-white">{self.report.scan_time_ms:,}ms</span>
                        </div>
                    </div>
                </div>
            </div>
        </aside>

        <!-- Main Content -->
        <main class="content flex-1 min-w-0">
            <div class="max-w-5xl mx-auto p-4 lg:p-10">
                
                <!-- Header -->
                <header id="overview" class="mb-10">
                    <div class="flex justify-between items-start mb-6">
                        <div>
                            <h1 class="text-3xl font-bold text-gray-900 mb-2">Analysis Report</h1>
                            <p class="text-gray-500 font-mono text-sm break-all">{self.report.file_path}</p>
                        </div>
                        <div class="hidden sm:block text-right">
                            <div class="text-sm text-gray-500">Generated on</div>
                            <div class="font-medium text-gray-900">{generated_date}</div>
                        </div>
                    </div>

                    <!-- Metrics Grid -->
                     <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                        <div class="bg-white p-5 rounded-xl border border-gray-200 shadow-sm">
                            <div class="text-sm font-medium text-gray-500 mb-1">Quality Score</div>
                            <div class="flex items-baseline gap-2">
                                <span class="text-3xl font-bold text-blue-600">{self.report.quality_score():.1f}%</span>
                            </div>
                        </div>
                         <div class="bg-white p-5 rounded-xl border border-gray-200 shadow-sm">
                            <div class="text-sm font-medium text-gray-500 mb-1">Total Variables</div>
                             <div class="flex items-baseline gap-2">
                                <span class="text-3xl font-bold text-gray-900">{self.report.total_columns}</span>
                            </div>
                        </div>
                         <div class="bg-white p-5 rounded-xl border border-gray-200 shadow-sm">
                            <div class="text-sm font-medium text-gray-500 mb-1">Total Rows</div>
                             <div class="flex items-baseline gap-2">
                                <span class="text-3xl font-bold text-gray-900">{self.report.total_rows:,}</span>
                            </div>
                        </div>
                         <div class="bg-white p-5 rounded-xl border border-gray-200 shadow-sm">
                            <div class="text-sm font-medium text-gray-500 mb-1">Memory Usage</div>
                             <div class="flex items-baseline gap-2">
                                <span class="text-3xl font-bold text-purple-600">Low</span>
                                <span class="text-xs text-gray-400">Streamed</span>
                            </div>
                        </div>
                    </div>
                </header>

                <hr class="border-gray-200 my-10" />

                <!-- Variables Section -->
                <section id="variables">
                    <div class="flex flex-col sm:flex-row justify-between items-end sm:items-center mb-6 gap-4">
                        <h2 class="text-2xl font-bold text-gray-900">Variables</h2>
                        <input type="text" id="searchInput" placeholder="Search variables..." class="w-full sm:w-64 px-4 py-2 rounded-lg border border-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm">
                    </div>

                    <div class="space-y-4" id="variablesContainer">
                        {variables_html}
                    </div>
                </section>

                <footer class="mt-20 pt-10 border-t border-gray-200 text-center text-gray-400 text-sm">
                    <p>Generated by <strong>DataProf</strong> - The High-Performance Data Profiler</p>
                </footer>
            </div>
        </main>
    </div>

    <!-- Simple Search Script -->
    <script>
        document.getElementById('searchInput').addEventListener('input', function(e) {{
            const term = e.target.value.toLowerCase();
            const cards = document.querySelectorAll('.variable-card');
            
            cards.forEach(card => {{
                const name = card.getAttribute('data-name');
                if (name.includes(term)) {{
                    card.style.display = 'block';
                }} else {{
                    card.style.display = 'none';
                }}
            }});
        }});
    </script>
</body>
</html>
        """
        return html


def profile(path: str) -> ProfileReport:
    """
    Profile a file and return a report object.
    
    Args:
        path: Path to the file (CSV, etc)
        
    Returns:
        ProfileReport: object containing the analysis and .save() method
    """
    # Simply call the quality analysis for now
    # Future versions could dispatch based on file extension
    report = analyze_csv_with_quality(path)
    return ProfileReport(report)

