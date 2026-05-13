<!--
Thanks for the contribution! Keep the description tight — what changed,
why, and how to verify. Delete sections that don't apply.
-->

## Summary

<!-- 1-3 sentences. What does this PR change and why? -->

## Type of change

<!-- Check one. Used for the changelog grouping. -->

- [ ] `feat` — new public-facing capability
- [ ] `fix` — bug fix (no API change)
- [ ] `perf` — performance improvement (no API change)
- [ ] `refactor` — internal restructuring (no behaviour change)
- [ ] `docs` — documentation only
- [ ] `test` — test or benchmark only
- [ ] `build` / `ci` — tooling, deps, CI

## Verification

<!--
Paste the relevant command output. The four lines below are the bar -
every PR must pass all of them.
-->

```bash
pytest --no-cov -q       # all tests pass
mypy incorporator/       # mypy --strict clean
ruff check incorporator/ tests/
black --check incorporator/ tests/
```

<!-- If you touched the hot path, also run: -->
<!-- pytest -m benchmark -->

## Notes for reviewers

<!--
Anything reviewers should know: trade-offs considered, alternatives
rejected, known limitations, follow-up tickets.
-->
