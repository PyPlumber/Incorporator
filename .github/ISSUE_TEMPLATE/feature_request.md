---
name: ✨ Feature request
about: Suggest a new capability or improvement
title: "[Feature] "
labels: enhancement
---

## What problem are you trying to solve?

<!--
Describe the user-facing scenario. "I want to X but currently have to Y."
Frame it as a problem, not a solution — the solution is for us to design
together.
-->

## What you're imagining

<!--
A rough sketch of how this would look from the user's perspective.
Pseudocode or an example pipeline.json is fine.
-->

```python
# example DX you'd like to see
```

## Alternatives you've considered

<!-- Workarounds, related tools, other approaches you've thought through. -->

## Scope check

- [ ] This change keeps Incorporator dict-native (storage is `List[Dict]`,
      not Arrow / pandas / NumPy in the hot path)
- [ ] This change does NOT add a required system dependency. Heavyweight
      libraries belong in an optional extra.
- [ ] This change is about Incorporator's library surface, not the
      external HTTP APIs Incorporator consumes.
