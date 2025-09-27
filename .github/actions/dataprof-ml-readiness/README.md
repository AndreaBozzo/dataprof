# DataProf ML Readiness Action

Internal composite action for ML readiness assessment in dataprof CI/CD workflows.

## Usage

```yaml
- name: ML Readiness Check
  uses: ./.github/actions/dataprof-ml-readiness
  with:
    file: 'examples/sample_data.csv'
    ml-threshold: 80
```

## Inputs

| Input | Description | Required | Default |
|-------|-------------|----------|---------|
| `file` | Path to CSV file to analyze | ✅ | - |
| `ml-threshold` | ML readiness threshold (0-100) | ❌ | `80` |
| `fail-on-issues` | Fail job if below threshold | ❌ | `true` |
| `output-format` | Output format (json/csv/text) | ❌ | `json` |
| `generate-code` | Generate code snippets | ❌ | `true` |

## Outputs

- `ml-score`: Overall ML readiness score
- `readiness-level`: HIGH/MEDIUM/LOW classification
- `recommendations`: JSON array of improvements
- `analysis-summary`: Human-readable summary

## Integration

This action integrates with existing dataprof infrastructure:

- Uses `setup-rust` action for dataprof-cli installation
- Leverages existing test CSV files in `examples/`
- Follows composite action patterns from other internal actions
- Compatible with existing CI workflows

## Examples

See `.github/workflows/test-action.yml` for comprehensive test cases.
