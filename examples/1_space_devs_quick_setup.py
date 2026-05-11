"""
Quick Setup Tutorial: Space Devs Launch Tracker
-----------------------------------------------
This example demonstrates how to use Incorporator to ingest a nested REST API,
handle pagination, clean the data payload, and automatically cast string
timestamps into Python datetime objects—all in a single function call.
"""

import asyncio
from datetime import datetime

from incorporator import Incorporator, IncorporatorList, NextUrlPaginator, inc


# 1. Define an empty class inheriting from Incorporator
class Launch(Incorporator):
    pass


# 2. Test API for kwargs
async def inspect_api():
    print("Inspecting the Space Devs API...")
    # Use .test() to map out unknown data!
    await Launch.test(inc_url="https://ll.thespacedevs.com/2.2.0/launch/upcoming/")


async def track_launches():
    print("Fetching Upcoming Space Launches...")

    # 3. Build the object graph dynamically
    launches: IncorporatorList[Launch] = await Launch.incorp(
        # Navigation & Targeting
        inc_url="https://ll.thespacedevs.com/2.2.0/launch/upcoming/",
        rec_path="results",  # Skip the metadata wrapper; target the launch array
        # Identity
        inc_code="id",  # Unique identifier for the memory registry
        inc_name="name",  # Human-readable label
        # Pagination & Limits
        inc_page=NextUrlPaginator("next"),  # Tell Incorporator how to find the next page
        call_lim=2,  # Limit to 2 API calls
        # Optimization
        excl_lst=["image", "infographic", "program", "vid_urls"],  # Drop heavy, unused JSON keys
        # Data Transformation (ETL)
        conv_dict={
            "net": inc(datetime)  # Natively cast ISO-8601 strings to datetime objects
        },
    )

    print(f"Successfully mapped {len(launches)} space launches!\n")

    # 3. Traverse the automatically generated object graph
    for launch in launches[:15]:
        print(f"🚀 {launch.inc_name}")

        # Because we used `inc(datetime)`, 'net' is a fully formatted datetime object
        print(f"   Date: {launch.net.strftime('%B %d, %Y at %H:%M UTC')}")

        # Incorporator dynamically builds nested subclasses.
        # The JSON returned {"pad": {"latitude": "...", "location": {"name": "..."}}}
        # We can traverse this using standard Python dot-notation instantly!
        if hasattr(launch, "pad"):
            lat = getattr(launch.pad, "latitude", "Unknown")
            lon = getattr(launch.pad, "longitude", "Unknown")
            region = getattr(launch.pad.location, "name", "Unknown") if hasattr(launch.pad, "location") else "Unknown"

            print(f"   📍 Region: {region}")
            print(f"   🗺️  GPS:    {lat}, {lon}")

        print("-" * 55)

    # 4. The IDE provides full autocomplete for the internal Memory Registry
    if launches:
        first_id = launches[0].inc_code

        # Type 'launches.' in your IDE and watch 'inc_dict' pop right up!
        cached_launch = launches.inc_dict.get(first_id)
        print(f"O(1) Memory Lookup Success: {cached_launch.inc_name}")


if __name__ == "__main__":
    #    asyncio.run(inspect_api())
    asyncio.run(track_launches())
