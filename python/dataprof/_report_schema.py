"""Serialized report version and quality dimension ordering."""

from __future__ import annotations as _annotations

# Version of the serialized report document written by to_dict()/to_json()/
# save(). Independent of the package version: it only changes when the report
# schema itself changes. Readers accept any document whose schema_version is
# at most this value; documents without the field are legacy pre-0.10 reports
# and load through a compatibility path. See ProfileReport.from_dict().
REPORT_SCHEMA_VERSION = 1

# The seven quality dimensions, in scoring order. Keeping one list rather than
# several is the point: duplicated copies of this exact set are what drifted.
_QUALITY_DIMENSIONS = (
    "completeness",
    "consistency",
    "uniqueness",
    "accuracy",
    "timeliness",
    "validity",
    "precision",
)
