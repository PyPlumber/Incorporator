"""
DX Inspector Tutorial: Let the Framework Write Your Kwargs
----------------------------------------------------------
Companion script for `docs/8_dx_inspector.md`.

`test()` is the JIT API Profiler. Hand it the URL of any endpoint and
it fetches one safe page, walks the payload tree, runs regex-based
value scoring to detect identity-shaped fields, and prints the exact
`incorp()` kwargs you'd write yourself — minus the trial and error.

Run with:
    python examples/8_dx_inspector.py
"""

import asyncio

from incorporator import Incorporator


class Launch(Incorporator):
    """Placeholder subclass — `test()` doesn't need a real schema declared."""


async def main() -> None:
    # Hit the unknown endpoint via test() to print the inspector report.
    # Safe: forces call_lim=1 + short timeout + max 3-record preview.
    await Launch.test(inc_url="https://api.spacexdata.com/v4/launches/latest")

    # The inspector prints suggested kwargs above. Once you've read the
    # report, copy them into a real incorp() call — uncomment below:
    #
    # launches = await Launch.incorp(
    #     inc_url="https://api.spacexdata.com/v4/launches/latest",
    #     inc_code="id",            # from "🔑 IDENTITY MAPPING"
    #     inc_name="name",
    # )
    # print(f"Loaded {len(launches)} launches: {launches[0].inc_name}")


if __name__ == "__main__":
    asyncio.run(main())
