# Pull Request

**👋 Thank you for contributing to dataprof!** 

Please fill out this template to help us understand your changes.

## 📝 Summary

Brief description of the changes in this PR. What problem does it solve or what feature does it add?

## 🔧 Type of Change

Select the type(s) of change:

- [ ] 🐛 Bug fix (non-breaking change that fixes an issue)
- [ ] ✨ New feature (non-breaking change that adds functionality)
- [ ] 🚨 Breaking change (would cause existing functionality to not work as expected)
- [ ] 📚 Documentation update
- [ ] ⚡ Performance improvement
- [ ] 🎨 Code refactoring (no functional changes)

## 📋 Changes Made

Describe the specific changes:

- Change 1
- Change 2
- Change 3

**Related Issue:** Closes #(issue number)

## 🧪 Testing

### How did you test this?

Describe your testing approach:

- [ ] Tested locally with `cargo test`
- [ ] Added new tests for this feature
- [ ] All existing tests pass
- [ ] Tested with sample data files
- [ ] Tested edge cases (empty files, large files, etc.)

### Test Commands Run

```bash
cargo test --all
cargo clippy --all --all-targets
cargo fmt --all -- --check
cargo build --release
```

Include any specific test scenarios or data used.

## 📖 Documentation

- [ ] Updated README.md if user-facing changes
- [ ] Added/updated inline code comments
- [ ] Updated docs/ folder if architectural changes
- [ ] No documentation changes needed

## ⚡ Performance Impact

- [ ] No performance impact
- [ ] Performance improved
- [ ] Performance may be affected (explain below)

**Details (if applicable):**

## 🚨 Breaking Changes

If this is a breaking change, describe:

- What breaks
- Migration path for users

**If no breaking changes, just check:** [ ] No breaking changes

## 📌 Additional Notes

Any additional context for reviewers:

- Screenshots/examples (if applicable)
- Related PRs or discussions
- Implementation notes

---

## ✅ Checklist

- [ ] Code builds without warnings
- [ ] No linting errors (`cargo clippy`)
- [ ] Code is formatted (`cargo fmt`)
- [ ] All tests pass
- [ ] Self-reviewed my code
- [ ] Comments added for complex logic
- [ ] No hardcoded secrets or sensitive data

**Need help?** See [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines and [docs/](../../docs/) for architecture details.

