\# DataProf 0.9.0 Release Notes



\## Breaking Changes



\### DataQualityMetrics Semantics

The `DataQualityMetrics` class has changed from flat to nested structure.



\*\*Before (0.8.x):\*\*

```python

metrics = profile.quality\_metrics()

print(metrics.completeness)

print(metrics.uniqueness)

