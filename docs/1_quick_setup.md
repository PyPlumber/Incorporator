***

# 🚀 Quick Setup: Building a Space Launch Tracker

Welcome to the **Incorporator** Quick Setup! 

When consuming REST APIs, developers usually have to write boilerplate Pydantic models, handle `while` loops for pagination, and write custom logic to convert JSON strings into native Python types. 

In this tutorial, we will use the **Space Devs Launch API** to see how Incorporator handles all of this automatically in a single asynchronous function call.

## The Goal
We want to fetch upcoming rocket launches, convert their launch times into Python `datetime` objects, and extract the GPS coordinates of their launch pads.

## ⚙️ Installation
If you haven't already, install Incorporator:
```bash
pip install incorporator
```

## 💻 The Code

Create a file called `track_launches.py` and drop in the following code:

```python
import asyncio
from datetime import datetime
from incorporator import Incorporator, IncorporatorList, NextUrlPaginator, inc

# 1. Define an empty class inheriting from Incorporator
class Launch(Incorporator): 
    pass

async def main():
    print("Fetching Upcoming Space Launches...")

    # 2. Type-hint IncorporatorList[T] to unlock perfect IDE autocomplete!
    # Your editor will now natively understand the framework's magic methods.
    launches: IncorporatorList[Launch] = await Launch.incorp(
        inc_url="https://ll.thespacedevs.com/2.2.0/launch/upcoming/",
        rec_path="results",
        inc_code="id",
        inc_name="name",
        inc_page=NextUrlPaginator("next"),
        call_lim=1, 
        excl_lst=["image", "infographic", "program", "vid_urls"], 
        conv_dict={"net": inc(datetime)} 
    )

    print(f"Successfully mapped {len(launches)} space launches!\n")

    # 3. Traverse the dynamically generated object graph
    for launch in launches[:5]:
        print(f"🚀 {launch.inc_name}")
        
        # 'net' is now a real datetime object!
        print(f"   Date: {launch.net.strftime('%B %d, %Y at %H:%M UTC')}")
        
        # Traverse nested JSON using standard Python dot-notation
        if hasattr(launch, 'pad'):
            lat = getattr(launch.pad, 'latitude', 'Unknown')
            lon = getattr(launch.pad, 'longitude', 'Unknown')
            
            # Incorporator built a nested subclass for 'location' automatically
            region = getattr(launch.pad.location, 'name', 'Unknown') if hasattr(launch.pad, 'location') else 'Unknown'

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
    asyncio.run(main())
```

---

## 🧠 Parameter Breakdown

The magic of Incorporator comes from the configuration passed to `.incorp()`. Here is exactly what each parameter does:

### 1. Navigation: `rec_path`
Most REST APIs wrap their data in metadata. The Space Devs API returns `{"count": 140, "next": "url...", "results": [...]}`. By setting `rec_path="results"`, we tell Incorporator to dive straight into the `"results"` array and ignore the useless pagination wrapper.

### 2. Identity: `inc_code` & `inc_name`
* `inc_code="id"` maps the unique identifier for Incorporator's internal memory registry. This prevents duplicate objects if data overlaps.
* `inc_name="name"` sets the human-readable label, allowing us to easily call `launch.inc_name`.

### 3. Pagination Control: `inc_page` & `call_lim`
Say goodbye to `while` loops. 
* `inc_page=NextUrlPaginator("next")` tells the framework to look for the `"next"` key in the root JSON response to find the URL for page 2.
* `call_lim=1` restricts the framework to exactly 1 API call.

### 4. Memory Optimization: `excl_lst`
The Space Devs API returns heavy image URLs and infographic links that our terminal app doesn't need. Passing `excl_lst=["image", "infographic", "program", "vid_urls"]` strips these keys *before* object creation, keeping your memory footprint minimal.

### 5. On-the-Fly Type Casting: `conv_dict` & `inc()`
JSON does not have a native `datetime` format; dates arrive as rigid ISO-8601 strings (e.g., `"2026-05-12T22:15:00Z"`). 
Instead of writing a manual parsing function, we use Incorporator's built-in `inc()` converter function:
```python
conv_dict={"net": inc(datetime)}
```
This intercepts the `"net"` (No Earlier Than) JSON key and cleanly casts it into a native, timezone-aware Python `datetime` object automatically.

---

## ✨ The Magic of Auto-Nesting

If you look closely at the `print` loop, you'll see we cleanly access `launch.pad.location.name`. 

In the raw API, the launch pad isn't flat data—it is a deeply nested dictionary that looks like this:
```json
"pad": {
    "id": 87,
    "latitude": "28.60822681",
    "longitude": "-80.60428186",
    "location": {
        "name": "Kennedy Space Center, FL, USA"
    }
}
```
In traditional frameworks, you would have to define explicit `PadModel` and `LocationModel` classes to type-hint and access this data. 

**Incorporator dynamically traverses nested JSON dictionaries and generates native Python sub-classes on the fly.** You get strict, object-oriented dot-notation with absolutely zero boilerplate setup required.
