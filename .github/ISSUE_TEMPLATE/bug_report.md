---
name: 🐛 Bug report
about: Something is broken or behaves unexpectedly
title: "[Bug] "
labels: bug
---

## What happened

<!-- A clear, concise description. -->

## What you expected

<!-- What should have happened instead? -->

## Minimal reproduction

<!--
Paste the smallest possible code (or pipeline.json) that triggers the bug.
We can't help without a reproduction. Strip anything that isn't required.
-->

```python
import asyncio
from incorporator import Incorporator

async def main():
    # ...

asyncio.run(main())
```

## Environment

- Incorporator version: `pip show incorporator | grep Version`
- Python version: `python --version`
- OS: <!-- e.g. macOS 14.4, Ubuntu 22.04, Windows 11 -->
- Installed extras: <!-- e.g. [speedups], [parquet], [all] -->

## Traceback / logs

<!-- Paste the full traceback or the relevant lines from logs/error.log -->

```
<traceback here>
```

## Anything else?

<!-- Workarounds you've tried, related issues, etc. -->
