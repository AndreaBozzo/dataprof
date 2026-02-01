# Changelog

All notable changes to DataProfiler will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.10] - 2026-02-01

### Added

- Add profile_arrow function for PyArrow Table and RecordBatch profiling (#199) by @AndreaBozzo

- Implement Arrow PyCapsule interface for zero-copy data exchange (#196) by @AndreaBozzo

### Changed

- Autofix n.26 (#198) by @AndreaBozzo

- Removed legacy req build txt and moved social docs to docs/ folder by @AndreaBozzo

- **Pages**: Add workflow to deploy benchmark reports to GitHub Pages (#195) by @AndreaBozzo

### Dependencies

- **Deps**: Bump thiserror from 2.0.17 to 2.0.18 (#184) by @dependabot[bot]

### Documentation

- Update readme by @AndreaBozzo

- Logo width 800 by @AndreaBozzo

### Fixed

- **Html**: Simplify compression ratio calculation in report context for clippy by @AndreaBozzo

- **Build**: Add anyhow to dev-dependencies, revert crate-type to rlib only by @AndreaBozzo

## [0.5.0] - 2026-01-18

### Added

- Add futures dependency and enhance DataFusionLoader with streaming query profiling by @AndreaBozzo

- Integrate DataFusion SQL (#177) by @AndreaBozzo

- **Logging**: Integrate env_logger for improved logging throughout the application ( bye emojis ) by @AndreaBozzo

- **CI**: Add code coverage workflow and remove tarpaulin configuration (testing, might revert) by @AndreaBozzo

- Enhance performance dashboard with interactive Chart.js visualizations and improved data handling by @AndreaBozzo

- Add linting configuration and improve chunk processing readability by @AndreaBozzo

- Refactor chunk processing to use ByteRecord and add batch processing with Rayon for improved performance by @AndreaBozzo

- Add interactive demo section to README and include demo animation by @AndreaBozzo

### Changed

- Replace info! macro with log::info for consistency in logging by @AndreaBozzo

- Remove clippy ( present in CI) and fine-tuning by @AndreaBozzo

- Increase timeout for supply-chain job to 45 minutes by @AndreaBozzo

- Remove cargo audit configuration and add deny.toml template by @AndreaBozzo

- Update CSV analysis functions to support optional engine parameter by @AndreaBozzo

- Remove API reference and integrations documentation for DataProf by @AndreaBozzo

- Remove outdated guides documentation by @AndreaBozzo

- Update version to 0.5.0 and adjust dependencies in Cargo files by @AndreaBozzo

- Fmt by @AndreaBozzo

- Removed example files and remade security workflow by @AndreaBozzo

- Remove integration tests from CI workflow matrix by @AndreaBozzo

- Remove setup of system dependencies from CI workflow by @AndreaBozzo

- Re-add simplified issue templates for bug reports and feature requests ( same as Ceres ) by @AndreaBozzo

- Update comments and add feature flags for arrow in integration regression tests by @AndreaBozzo

- Rename IsoQualityThresholds to IsoQualityConfig for consistency by @AndreaBozzo

- Rename 'with_*' methods to 'seed' and 'limits' for clarity by @AndreaBozzo

- Standardize database analysis naming (profile_* → analyze_*) by @AndreaBozzo

- Add *.so to .gitignore to exclude shared object files by @AndreaBozzo

- Reorganize version import in Python bindings by @AndreaBozzo

- Remove SimpleColumnarProfiler and update integration tests to use ArrowProfiler by @AndreaBozzo

- Remove deprecated development container files and update references to docker-compose by @AndreaBozzo

- Improvedevelopment databases and testing scripts by @AndreaBozzo

- **Metrics**: Split metrics into separate dimensions by @AndreaBozzo

- Remove unused streaming arguments and related tests for analyze and report commands by @AndreaBozzo

- Update toml dependency to version 0.9.8 by @AndreaBozzo

- Clippy & fmt by @AndreaBozzo

- **Tests**: Remove outdated testing modules and legacy tests by @AndreaBozzo

- Replace println! with log statements for improved logging consistency, followup of 7c5a802 by @AndreaBozzo

- **Docs, CI**: Add Codecov token to coverage upload and update README with Codecov badge by @AndreaBozzo

- Add sparse protocol configuration for crates.io by @AndreaBozzo

- Removed redundant docstrings by @AndreaBozzo

- Remove pre-commit configuration file by @AndreaBozzo

- More docs cleanup, this folder will be phased out when i finish transitioning to testcontainers by @AndreaBozzo

- Removed unused bench folder by @AndreaBozzo

- Remove outdated README.md for GitHub Actions workflows by @AndreaBozzo

- Update license badge to reflect dual-license under MIT and Apache 2.0 by @AndreaBozzo

- Update license information to dual-license under MIT and Apache 2.0; add LICENSE-APACHE file by @AndreaBozzo

- Update changelog to reflect correct project name and removed those stupid emojis from workflow, this will be an ongoing effort by @AndreaBozzo

- Remove VSCode snippets and workspace configuration files by @AndreaBozzo

- Renamed old test file by @AndreaBozzo

- Remove link to full contributing guide from README by @AndreaBozzo

- Streamline error handling and row processing in database connectors by @AndreaBozzo

- Fmt by @AndreaBozzo

- Update Cargo.toml profile and enhance benchmarks documentation by @AndreaBozzo

### Dependencies

- **Deps**: Bump trufflesecurity/trufflehog from 3.83.7 to 3.92.4 (#162) by @dependabot[bot]

- **Deps**: Bump parquet from 57.1.0 to 57.2.0 (#173) by @dependabot[bot]

- **Deps**: Bump arrow from 57.1.0 to 57.2.0 (#172) by @dependabot[bot]

- **Deps**: Bump predicates from 2.1.5 to 3.1.3 (#174) by @dependabot[bot]

- **Deps**: Bump tokio from 1.48.0 to 1.49.0 (#175) by @dependabot[bot]

- **Deps**: Bump toml from 0.9.8 to 0.9.10+spec-1.1.0 (#158) by @dependabot[bot]

### Documentation

- Eliminated redundant doc by @AndreaBozzo

- Update section title in Code of Conduct to reflect commitment by @AndreaBozzo

### Fixed

- Update CI workflow to skip test suite ( handled by coverages tests, this saves me computing ) on master pushes by @AndreaBozzo

- Update GitHub Actions workflows to use latest action versions and merge coverage reporting ( no need to run multiple tests runs ) by @AndreaBozzo

- Update code coverage generation and upload configuration by @AndreaBozzo

- Update dependencies and improve security audit configuration by @AndreaBozzo

- Correct capitalization in TruffleHog ignore file header by @AndreaBozzo

- Update copyright year in LICENSE file to 2026 by @AndreaBozzo

- Reorder imports to include MemoryMappedCsvReader for memory mapping tests by @AndreaBozzo

- Remove unnecessary Claude files from .gitignore and add mypy cache by @AndreaBozzo

- Add "analyze" argument to subprocess calls in benchmark scripts ( yikes ) by @AndreaBozzo

- Adjust placeholder replacement order in performance dashboard generation script by @AndreaBozzo

- Update report links to use the correct directory structure and add timestamp generation in benchmarks workflow by @AndreaBozzo

- Enable LTO in release profile and adjust crate-type for library configuration by @AndreaBozzo

- Comment out lto in release profile to prevent linking issues with mixed rlib/cdylib by @AndreaBozzo

- Optimize release profile settings for improved performance, removed useless docstrings by @AndreaBozzo

## [0.4.85] - 2026-01-09

### Added

- Overhaul HTML UI, fix Python bindings, and update docs (#168) by @AndreaBozzo

### Changed

- Prepare release 0.4.85 with git-cliff integration by @AndreaBozzo

- Rename fields in JSON context for clarity in batch report by @AndreaBozzo

### Dependencies

- **Deps**: Bump tempfile from 3.23.0 to 3.24.0 (#161) by @dependabot[bot]

- **Deps**: Bump rsa in the cargo group across 1 directory (#167) by @dependabot[bot]

- **Deps**: Bump handlebars from 6.3.2 to 6.4.0 (#166) by @dependabot[bot]

### Fixed

- Add unsafe to Windows extern block and exclude large files from crate by @AndreaBozzo

- **Ci**: Use git-cliff action correctly with outputs by @AndreaBozzo

- **Ci**: Use git-cliff instead of git cliff (correct command) by @AndreaBozzo

- **Ci**: Add debug output for git-cliff and use --current flag by @AndreaBozzo

- Rust 2024 unsafe extern block and update Cargo.lock by @AndreaBozzo

- Remove unnecessary panic setting in release profile by @AndreaBozzo

- Update dataprofhtml2026.png image asset cropping by @AndreaBozzo

## [0.4.84] - 2026-01-02

### Changed

- Bump version to 0.4.84 and update changelog by @AndreaBozzo

- Ease external PR contribution workflow (#165) by @AndreaBozzo

### Dependencies

- **Deps**: Bump actions/upload-artifact from 5 to 6 (#163) by @dependabot[bot]

- **Deps**: Bump actions/download-artifact from 6 to 7 (#164) by @dependabot[bot]

- **Deps**: Bump parquet from 57.0.0 to 57.1.0 by @dependabot[bot]

- **Deps**: Bump log from 0.4.28 to 0.4.29 by @dependabot[bot]

- **Deps**: Bump criterion from 0.7.0 to 0.8.1 by @dependabot[bot]

- **Deps**: Bump arrow from 57.0.0 to 57.1.0 by @dependabot[bot]

- **Deps**: Bump wide from 0.8.3 to 1.1.0 by @dependabot[bot]

- **Deps**: Bump actions/upload-artifact from 4 to 5 by @dependabot[bot]

- **Deps**: Bump actions/checkout from 5 to 6 by @dependabot[bot]

- **Deps**: Bump pyo3 from 0.27.1 to 0.27.2 by @dependabot[bot]

### Documentation

- Remove batch report animation from README by @AndreaBozzo

- Update logo size in README for better visibility by @AndreaBozzo

- Update README with new logo and improved project description by @AndreaBozzo

- Add enhanced statistical analysis features and remove CI/CD section by @AndreaBozzo

### Fixed

- **Ci**: Replace deprecated macos-13 with macos-15-intel by @AndreaBozzo

## [0.4.83] - 2025-11-28

### Added

- Add precision control for numeric output formatting by @AndreaBozzo

- Implement enhanced statistical analysis (issue #139) by @AndreaBozzo

### Changed

- Update Cargo.lock for version 0.4.83 by @AndreaBozzo

- Bump version to 0.4.83 and update changelog by @AndreaBozzo

### Dependencies

- **Deps**: Bump clap from 4.5.51 to 4.5.53 (#142) by @dependabot[bot]

- **Deps**: Bump wide from 0.8.2 to 0.8.3 (#141) by @dependabot[bot]

- **Deps**: Bump indicatif from 0.18.2 to 0.18.3 (#140) by @dependabot[bot]

### Fixed

- Update release title to use lowercase 'dataprof' and remove generated comment by @AndreaBozzo

- Remove unnecessary blank line in quartiles module by @AndreaBozzo

- Address Copilot review comments by @AndreaBozzo

- Pre-PR improvements for code quality and compatibility by @AndreaBozzo

## [0.4.82] - 2025-11-21

### Added

- Enhance pattern detection with 11 new patterns and performance improvements by @AndreaBozzo

### Changed

- Update version to 0.4.82 and add release notes in changelog by @AndreaBozzo

- **Changelog**: Update dependencies section with GitHub Actions and Rust updates and removed outdated notebooks note by @AndreaBozzo

- Improve clarity in staging to master workflow comments and messages by @AndreaBozzo

### Dependencies

- **Deps**: Bump actions/upload-pages-artifact from 3 to 4 (#130) by @dependabot[bot]

- **Deps**: Bump actions/download-artifact from 4 to 6 (#129) by @dependabot[bot]

- **Deps**: Bump wide from 0.8.1 to 0.8.2 (#135) by @dependabot[bot]

- **Deps**: Bump github/codeql-action from 3 to 4 (#131) by @dependabot[bot]

### Fixed

- Disable on push and pull_request triggers in staging workflow by @AndreaBozzo

## [0.4.81] - 2025-11-15

### Added

- Add code coverage workflow and configuration by @AndreaBozzo

### Changed

- Minor fixes from project review by @AndreaBozzo

### Fixed

- Standardize color value to 'Auto' in tarpaulin configuration by @AndreaBozzo

- Update timeout values to include 's' suffix for consistency by @AndreaBozzo

- Update tarpaulin configuration for coverage outputs and simplify coverage command by @AndreaBozzo

## [0.4.80] - 2025-11-07

### Added

- **Async**: Re-enable and improve async support (issue #133) by @AndreaBozzo

### Changed

- Re-added cargo.lock by @AndreaBozzo

- Bump version to 0.4.80 and update changelog by @AndreaBozzo

- Applied fmt by @AndreaBozzo

- Upgrade major dependencies and migrate to modern APIs by @AndreaBozzo

## [0.4.78] - 2025-10-23

### Changed

- Bump version to 0.4.78 by @AndreaBozzo

### Documentation

- Fixed typo in readme.md by @AndreaBozzo

### Fixed

- Enable PyO3 forward compatibility for Python 3.14+ by @AndreaBozzo

- Apply Copilot PR suggestions by @AndreaBozzo

## [0.4.77] - 2025-10-16

### Added

- Update version to 0.4.77 - added lazy static - pre-comp regex for better performances - removed some unwrap calls by @AndreaBozzo

### Changed

- Trimmed snippets and ws config couse my setup is slow by @AndreaBozzo

### Documentation

- Update CHANGELOG for v0.4.77 by @AndreaBozzo

- Update documentation for v0.4.75 with Parquet batch support by @AndreaBozzo

### Fixed

- Apply valid Copilot suggestions by @AndreaBozzo

- Apply Copilot suggestions - IT phone regex and total_cmp by @AndreaBozzo

- Clippy fix by @AndreaBozzo

## [0.4.75] - 2025-10-09

### Added

- **Python**: Complete feature parity with Rust core for batch Parquet support by @AndreaBozzo

- **Parquet**: Production-ready implementation with extended type coverage by @AndreaBozzo

- **Parquet**: Add Apache Parquet format support with unified API integration by @AndreaBozzo

- Add JSON batch export functionality and clean up formatters by @AndreaBozzo

### Changed

- Update Cargo keywords for crates.io compliance by @AndreaBozzo

- Post-first-month cleanup - Phase 1 & 2 complete by @AndreaBozzo

- Re-organized examples directory by @AndreaBozzo

- **Config**: Technical debt cleanup - consolidate structures by @AndreaBozzo

- **Parsers**: Eliminate expensive parser cloning + remove deprecated field by @AndreaBozzo

- **Engines**: Consolidate streaming statistics - eliminate duplication by @AndreaBozzo

- **Core**: Major architectural improvements - Priority High by @AndreaBozzo

### Documentation

- Release version 0.4.75 by @AndreaBozzo

- Update package description to highlight ISO quality metrics by @AndreaBozzo

### Fixed

- **Parquet**: Resolve conditional compilation for is_parquet_file by @AndreaBozzo

- **Parquet**: Resolve feature gate compilation error in core_logic by @AndreaBozzo

- More imports fixes by @AndreaBozzo

- Fixed 2 missing imports for CI by @AndreaBozzo

## [0.4.70] - 2025-10-03

### Added

- Add batch ML/HTML support and fix quality score calculation by @AndreaBozzo

- Complete CLI subcommands refactor with backward compatibility (Phase 4/4) ✅ by @AndreaBozzo

- Add ISO 8000/25012 configurable quality metrics and CLI refactor design by @AndreaBozzo

- Unify database mode to use full MlReadinessEngine by @AndreaBozzo

- Enhanced ML readiness analysis with advanced feature intelligence by @AndreaBozzo

- Complete PyDataQualityMetrics integration and database ML pipeline by @AndreaBozzo

- Complete DataQualityMetrics display integration by @AndreaBozzo

- Expose ML features and output options in Python batch functions by @AndreaBozzo

- Complete batch ML processing integration + critical ML score fix by @AndreaBozzo

- Enable batch processing functionality + CLI validation fix by @AndreaBozzo

- Enhanced User Experience & Terminal Intelligence - Issue #79 by @AndreaBozzo

### Changed

- Completed html refactor, added premade templates and a new gif and a dedicated metric test by @AndreaBozzo

- Complete cleanup - remove all legacy quality code by @AndreaBozzo

- Migrate HTML generation to Handlebars template engine (WIP) by @AndreaBozzo

- Massive architecture cleanup - eliminate tech debt (~730 lines) by @AndreaBozzo

- Eliminate code duplication and fix all clippy warnings by @AndreaBozzo

- Remove legacy code and simplify architecture (~2400 lines removed) by @AndreaBozzo

- Add shared core logic for CLI commands to inherit improvements by @AndreaBozzo

- Cleanup legacy code and enhance transparency documentation by @AndreaBozzo

### Documentation

- Docs update round before 0.4.70 release by @AndreaBozzo

- Update CHANGELOG with unified database ML implementation by @AndreaBozzo

### Fixed

- Fix ISO compliance tests by meeting minimum sample size requirements by @AndreaBozzo

- Ignore legacy integration tests with outdated expectations by @AndreaBozzo

- Fix VSCode config, separate heavy DB tests, rewrite CLI tests for new subcommand structure by @AndreaBozzo

- I'm out of tears for today pls pass by @AndreaBozzo

- Complete CI failures - align with QualityReport refactor by @AndreaBozzo

- Critical HTML quality score bug + complete Timeliness support by @AndreaBozzo

- Complete QualityIssue removal - establish single source of truth by @AndreaBozzo

- Critical bugfix - quality score calculation (outlier_ratio & stale_data_ratio) by @AndreaBozzo

- Complete Python bindings with timeliness metrics (ISO 8000-8) by @AndreaBozzo

- Calculate actual total_rows and quality metrics in streaming profiler by @AndreaBozzo

- Update tests for timeliness metrics (ISO 8000-8) by @AndreaBozzo

- Correct all command implementations with proper types (Phase 3/4) by @AndreaBozzo

- Clippy warnings in script_generator by @AndreaBozzo

- Benchmakr comp and html reports for single files, batch and db tests to come after by @AndreaBozzo

- Add clippy allow for too_many_arguments in Python batch functions by @AndreaBozzo

- Fixed both BatchConfig initializations in the Python batch processing module by adding the missing required fields by @AndreaBozzo

- Improve ML readiness scoring algorithm for realistic assessments by @AndreaBozzo

- Enhanced delimiter detection + complete test suite fixes by @AndreaBozzo

- CRITICAL - Resolve delimiter detection bug and enhance documentation by @AndreaBozzo

- Prevent benchmark workflow from running on feature branch pushes by @AndreaBozzo

- Add missing return value in Linux memory detection by @AndreaBozzo

- Resolve benchmark compilation errors by @AndreaBozzo

- Resolve compilation errors across test files and database connectors by @AndreaBozzo

## [0.4.61] - 2025-09-27

### Added

- Migrate to dual MIT OR Apache-2.0 licensing by @AndreaBozzo

### Changed

- Bump version to 0.4.61 by @AndreaBozzo

- Simplify to MIT License only by @AndreaBozzo

### Documentation

- Reorganize README with CI/CD integration and improved CLI documentation by @AndreaBozzo

## [0.4.6] - 2025-09-26

### Added

- Optimize CI/CD workflows for 30-40% performance improvement by @AndreaBozzo

- Add comprehensive demo notebook for v0.4.6 code snippet features by @AndreaBozzo

- Enhanced ML Recommendations with Actionable Code Snippets - Issue #71 by @AndreaBozzo

- Complete Phase 3 & 4 development environment standardization by @AndreaBozzo

- **Devenv**: Implement standardized development environment (Phase 1 & 2) by @AndreaBozzo

- Add workflow_dispatch trigger to quick benchmarks by @AndreaBozzo

- Implement comprehensive statistical rigor framework (#60) by @AndreaBozzo

- Consolidate and modernize benchmarking infrastructure by @AndreaBozzo

### Changed

- Create concise professional README under 80 lines by @AndreaBozzo

- Split benchmarks into parallel jobs for better CI performance by @AndreaBozzo

- Optimize CI workflows for GitHub standard runners by @AndreaBozzo

- Bump version to 0.4.6 with security enhancements by @AndreaBozzo

- Major CI/CD workflow optimization with composite actions by @AndreaBozzo

- Deleted justfile as i'm not using it by @AndreaBozzo

- Modularize database security.rs for better maintainability by @AndreaBozzo

- Modularize python.rs into organized module structure by @AndreaBozzo

- Modularize main.rs into organized CLI structure by @AndreaBozzo

### Documentation

- Fix documentation links in README on master by @AndreaBozzo

- Fix documentation links in README by @AndreaBozzo

- Update workflows README for optimized architecture by @AndreaBozzo

- Add comprehensive benchmarking system documentation by @AndreaBozzo

- Update changelog with CI/CD improvements by @AndreaBozzo

- Moved guides toa proper docs/ folder by @AndreaBozzo

- Update changelog with refactoring improvements by @AndreaBozzo

- Removed duplicate wiki files by @AndreaBozzo

### Fixed

- Remove empty PYO3_CROSS_LIB_DIR causing macOS build failure by @AndreaBozzo

- Update CHANGELOG.md version 0.4.6 from Unreleased status by @AndreaBozzo

- Correct integration test execution in staging-dev workflow by @AndreaBozzo

- Correct matrix variable access in CI workflow by @AndreaBozzo

- Replace deprecated crates extension with dependi by @AndreaBozzo

- Resolve clippy warnings in script generator by @AndreaBozzo

- Update demo notebook and type stubs for v0.4.6 code snippets by @AndreaBozzo

- Make setup scripts safer and remove problematic tool installations by @AndreaBozzo

- Refined .vscode workspace with better rust analyzer config and removed problematic powershell dev script for now by @AndreaBozzo

- Re-enabled CI checks on staging PRs couse i'm paranoid by @AndreaBozzo

- Make setup scripts safer and remove problematic tool installations by @AndreaBozzo

- Eliminate justfile completely and migrate to standard cargo commands by @AndreaBozzo

- Trimmed pre-commit to only feature code quality checks, while we shift the testing to the gh runners by @AndreaBozzo

- Show only real data in dashboard, remove fake placeholders by @AndreaBozzo

- Replace broken sed with perl for dashboard placeholder replacement by @AndreaBozzo

- Remove malformed Python multiline from YAML workflow by @AndreaBozzo

- Create functional dashboard with real benchmark data by @AndreaBozzo

- Replace custom benchmark actions with direct commands by @AndreaBozzo

- Resolve security workflow issues by @AndreaBozzo

- Update advanced security workflow to resolve CI failures by @AndreaBozzo

- Update TruffleHog configuration for proper regex patterns by @AndreaBozzo

- Resolve clippy warnings and update CI for deprecated PyO3 APIs by @AndreaBozzo

- Remove system file caching to prevent permission warnings by @AndreaBozzo

- Correct shell syntax in composite actions by @AndreaBozzo

- Increase sample-size to meet Criterion minimum requirement by @AndreaBozzo

- Resolve CI network issues and optimize quick benchmarks by @AndreaBozzo

- Resolve Criterion sample size assertion failure in statistical benchmarks by @AndreaBozzo

- Resolve failing test suite issues by @AndreaBozzo

- Replace unsafe unwrap() calls with safe alternatives by @AndreaBozzo

- Resolve all clippy errors and improve engine architecture by @AndreaBozzo

- Resolve clippy compilation errors in engine benchmarks by @AndreaBozzo

- Add missing Basic pattern support to streaming benchmarks by @AndreaBozzo

- Resolve domain pattern matching issue with proper module imports by @AndreaBozzo

- Resolve enum pattern matching issue in streaming domain by @AndreaBozzo

- Improve pattern matching in streaming domain generation by @AndreaBozzo

- Resolve unwrap() calls and benchmark compilation issues by @AndreaBozzo

- Added new justfile by @AndreaBozzo

## [0.4.53] - 2025-09-20

### Changed

- Fix Cargo.lock line endings and version by @AndreaBozzo

- Update dependencies and gitignore by @AndreaBozzo

- Update Cargo.lock for version 0.4.5 by @AndreaBozzo

### Fixed

- PyPI wheel naming compatibility by @AndreaBozzo

- Ensure RUSTFLAGS apply to all target architectures by @AndreaBozzo

- Resolve clippy dead_code warning by @AndreaBozzo

- Resolve CPU compatibility issues with Python wheels by @AndreaBozzo

## [0.4.5] - 2025-09-19

### Added

- Enhance GitHub Pages dashboard and benchmark workflows by @AndreaBozzo

### Changed

- Bump version to 0.4.5 for security release by @AndreaBozzo

- Enhance GitHub Pages dashboard visual design by @AndreaBozzo

### Documentation

- Adjusted version for changelog.md by @AndreaBozzo

- Added new Wiki pages by @AndreaBozzo

### Fixed

- Resolve CI test failures and feature compilation issues by @AndreaBozzo

- Remove unsafe unwrap() call in security module by @AndreaBozzo

- Address critical SQL injection and information disclosure vulnerabilities by @AndreaBozzo

## [0.4.4] - 2025-09-19

### Added

- Complete Python bindings enhancement for ML/AI workflows (#40) by @AndreaBozzo

- Implement comprehensive CLI enhancements for GitHub issue #39 by @AndreaBozzo

- Implement comprehensive CLI improvements with ML readiness scoring by @AndreaBozzo

- Optimize Cargo.toml build profiles and fix unwrap calls for faster compilation by @AndreaBozzo

- Add comprehensive database enhancements with ML readiness assessment by @AndreaBozzo

### Changed

- Update GitHub Actions workflows and add CLI integration tests by @AndreaBozzo

### Documentation

- Update CHANGELOG.md with Python bindings ML/AI enhancements by @AndreaBozzo

- Restructure Python documentation with organized wiki structure by @AndreaBozzo

- Removed markdownlint.yaml as is not being utilized. by @AndreaBozzo

- Added CLI guide wiki page by @AndreaBozzo

- Updated CHANGELOG.md to include last 2 PRs merged. by @AndreaBozzo

### Fixed

- Improve security scanner regex for more accurate detection by @AndreaBozzo

- Remove secret keyword from test to pass security scanner by @AndreaBozzo

- Improve security test to avoid scanner warnings by @AndreaBozzo

- Resolve security scanner warnings in test passwords by @AndreaBozzo

- Replace hardcoded test password with generic placeholder by @AndreaBozzo

- Remove broken dataprof-wiki submodule by @AndreaBozzo

- Re-applied linting by @AndreaBozzo

- Resolve panic strategy conflict for benchmarks by @AndreaBozzo

- Add missing public exports for CLI binary linking by @AndreaBozzo

- Restore original thiserror v2.0 configuration by @AndreaBozzo

- Downgrade thiserror to v1.0 for better compatibility by @AndreaBozzo

- Replace unsafe unwrap() calls with safe pattern matching by @AndreaBozzo

- Use is_multiple_of() instead of manual modulo check by @AndreaBozzo

- Applied formatting to python.rs by @AndreaBozzo

- Improve git operations resilience in release workflow by @AndreaBozzo

- Remove all unwrap() calls from progress.rs for safer error handling by @AndreaBozzo

- Complete CLI enhancements and resolve all test failures by @AndreaBozzo

- Resolve linking conflicts by using rlib crate type only by @AndreaBozzo

- Remove problematic build rustflags that caused linking errors by @AndreaBozzo

- Optimize staging-dev workflow to use default database features by @AndreaBozzo

- Clarify database features are default, not optional + optimize CI workflows by @AndreaBozzo

- Update DatabaseConfig initialization in database_examples.rs by @AndreaBozzo

- Relax performance test threshold to 3.0x for CI environments by @AndreaBozzo

## [0.4.1] - 2025-09-16

### Added

- Complete Arrow integration and intelligent engine selection by @AndreaBozzo

- Validate and refine performance claims with comprehensive benchmarking by @AndreaBozzo

### Changed

- Add Python artifacts to .gitignore by @AndreaBozzo

- Remove legacy setup.py files by @AndreaBozzo

### Documentation

- Update CLAUDE.md with essential commands by @AndreaBozzo

- Add v0.4.1 changelog for intelligent engine selection by @AndreaBozzo

### Fixed

- Relax performance test threshold for CI environments by @AndreaBozzo

- Removed windows path for linux ci runner by @AndreaBozzo

- Formatting cleanup by @AndreaBozzo

- Make engine selection tests deterministic by @AndreaBozzo

- Improve engine selection test for large files by @AndreaBozzo

- Resolve cross-platform binary path issue in benchmark script by @AndreaBozzo

- Improve GitHub Pages dashboard template and update CHANGELOG by @AndreaBozzo

- Add fallback return in memory usage detection for Linux by @AndreaBozzo

## [0.4.0] - 2025-09-14

### Added

- Implement comprehensive performance benchmarking suite - resolves #23 by @AndreaBozzo

- Improve release workflow with comprehensive automation by @AndreaBozzo

- Comprehensive test coverage infrastructure - issue #21 by @AndreaBozzo

### Changed

- Bump version to 0.4.0 with comprehensive changelog by @AndreaBozzo

- Implement modular architecture for lib.rs - resolves issue #20 by @AndreaBozzo

### Documentation

- Update changelog with comprehensive quality assurance features by @AndreaBozzo

- Redesign README for professional elegance and clarity by @AndreaBozzo

- Add Apache Arrow integration guide and update CHANGELOG v0.3.6 by @AndreaBozzo

- Add Code of Conduct for community guidelines by @AndreaBozzo

### Fixed

- Eliminate environment variable dependency for binary upload by @AndreaBozzo

- Add shell bash for wheel upload on Windows by @AndreaBozzo

- Improve binary preparation for cross-platform release by @AndreaBozzo

- Resolve GitHub Pages 404 for benchmark results by @AndreaBozzo

- Update GitHub Pages deployment to use actions/deploy-pages@v4 by @AndreaBozzo

- Updated benchmarks artifact generation on a specifical github page by @AndreaBozzo

- Update benchmark workflow triggers and criterion output format by @AndreaBozzo

- Use standard GitHub Actions runners instead of custom labels by @AndreaBozzo

- Resolve criterion sample-size requirement and workflow skipping behavior by @AndreaBozzo

## [0.3.6] - 2025-09-11

### Added

- Replace broken workflows with simple, human-logic release workflow by @AndreaBozzo

### Fixed

- Add --find-interpreter flag to maturin build by @AndreaBozzo

- Correct matrix strategy syntax in release workflow by @AndreaBozzo

## [0.3.6.1] - 2025-09-11

### Changed

- Update Cargo.lock for version 0.3.6 [skip ci] by @AndreaBozzo

- **Release**: Bump version to 0.3.6 [skip ci] by @AndreaBozzo

- **Release**: V0.3.6 [skip ci] by @AndreaBozzo

- Update gitignore for Node.js dependencies by @AndreaBozzo

### Dependencies

- **Deps**: Bump thiserror from 1.0.69 to 2.0.16 (#14) by @dependabot[bot]

### Fixed

- Resolve duplicate thiserror entries in Cargo.lock by @AndreaBozzo

## [0.3.5] - 2025-09-08

### Added

- Enhance release automation and fix crates.io publishing by @AndreaBozzo

- Optimize memory allocations and add leak detection for v0.3.5 by @AndreaBozzo

- Add comprehensive database connector system by @AndreaBozzo

- Setup staging branch development workflow by @AndreaBozzo

### Changed

- Update to v0.3.5 - Database Connectors & Memory Safety Edition by @AndreaBozzo

- Simplify staging-to-master workflow and add release automation by @AndreaBozzo

- Add comprehensive memory leak detection tests by @AndreaBozzo

- Temporarily disable Python features on macOS by @AndreaBozzo

- Apply code formatting and linter fixes by @AndreaBozzo

### Fixed

- Handle existing versions gracefully in both PyPI and crates.io by @AndreaBozzo

- Resolve compilation errors and security audit false positive by @AndreaBozzo

- Support SQLite :memory: connections and clarify sampling key usage by @AndreaBozzo

- Resolve streaming profiler test failures by @AndreaBozzo

- Finalize workflows and formatting for v0.3.5 release by @AndreaBozzo

- Replace unsafe unwrap() calls with proper error handling by @AndreaBozzo

- Resolve --all-features conflicts and simplify CI workflows by @AndreaBozzo

- Resolve Clippy warnings and align pre-commit hooks with CI workflows by @AndreaBozzo

- Resolve DuckDB linking issues in CI by testing features separately by @AndreaBozzo

- Correct unwrap() detection logic in CI workflow by @AndreaBozzo

- Update CI workflows for database features compatibility by @AndreaBozzo

- Remove unsafe unwrap() calls throughout codebase by @AndreaBozzo

- Use GitHub Actions conditionals instead of bash syntax by @AndreaBozzo

- Use correct PyO3 0.24+ Bound API pattern by @AndreaBozzo

- Add explicit PyO3 imports for v0.24+ compatibility by @AndreaBozzo

- Remove all unsafe unwrap() calls - resolves #18 by @AndreaBozzo

- Reduce clippy strictness in staging workflow by @AndreaBozzo

### Security

- Upgrade PyO3 to v0.24.2 to fix RUSTSEC-2025-0020 by @AndreaBozzo

## [0.3.0] - 2025-09-06

### Added

- Comprehensive v0.3.0 development infrastructure and tooling by @AndreaBozzo

- Implement v0.3.0 streaming and memory-efficient processing by @AndreaBozzo

### Dependencies

- **Deps**: Bump colored from 2.2.0 to 3.0.0 by @dependabot[bot]

- **Deps**: Bump clap from 4.5.46 to 4.5.47 by @dependabot[bot]

### Fixed

- Resolve all clippy warnings and pre-commit issues by @AndreaBozzo

- Apply pre-commit formatting and linting fixes by @AndreaBozzo

## [0.2.0] - 2025-09-03

### Added

- Implement comprehensive data quality analysis system by @AndreaBozzo

## [0.1.0] - 2025-09-02

