import pytest

from incorporator import Incorporator
from incorporator.schema.extractors import as_list, each


@pytest.mark.asyncio
async def test_live_post_tokens() -> None:
    """Tests live API POST functionality for both Iterative and Bulk Batch tokens.

    State isolation: subclasses are defined INSIDE the test function so each
    invocation gets a fresh ``inc_dict`` and ``_schema_union``. Module-level
    class definitions would accumulate state across test runs and could cause
    cross-test pollution under pytest-randomly or parallel execution.
    """

    class User(Incorporator):
        pass

    class Post(Incorporator):
        pass

    class EchoMirror(Incorporator):
        pass

    # ------------------------------------------
    # PHASE 1: Fetch Parent Data (10 Mock Users)
    # ------------------------------------------
    users = await User.incorp(inc_url="https://jsonplaceholder.typicode.com/users", inc_code="id", inc_name="name")

    assert len(users) == 10

    # ------------------------------------------
    # TEST 1: The `each()` Token
    # ------------------------------------------
    created_posts = await Post.incorp(
        inc_url="https://jsonplaceholder.typicode.com/posts",
        inc_parent=users,
        inc_child="id",
        http_method="POST",
        json_payload={
            "userId": each(),  # <--- THE TOKEN: Spawns 10 concurrent HTTPX calls
            "title": "Automated Incorporator Post",
            "body": "Look at this zero-boilerplate concurrency.",
        },
    )

    # We should get a list of 10 newly created Post objects back!
    assert isinstance(created_posts, list)
    assert len(created_posts) == 10

    # Verify the IDs were distributed correctly
    user_ids_used = [p.userId for p in created_posts]
    assert 1 in user_ids_used
    assert 10 in user_ids_used

    # ------------------------------------------
    # TEST 2: The `as_list()` Token
    # ------------------------------------------
    echo_response = await EchoMirror.incorp(
        inc_url="https://postman-echo.com/post",
        inc_parent=users,
        inc_child="id",
        http_method="POST",
        json_payload={
            "framework": "Incorporator v1.0.0",
            "batch_ids": as_list(),  # <--- THE TOKEN: Injects [1,2,3,4...10] into the JSON body
        },
        rec_path="json",  # Drill into the mirror's response to get our payload back
    )

    # Because it was 1 Bulk request, the framework returns a single Object (not a list)!
    assert not isinstance(echo_response, list)

    # Verify the server mirrored our exact framework name
    assert echo_response.framework == "Incorporator v1.0.0"

    # Verify the server mirrored a single array containing all 10 IDs!
    mirrored_array = echo_response.batch_ids
    assert isinstance(mirrored_array, list)
    assert len(mirrored_array) == 10
    assert mirrored_array == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
