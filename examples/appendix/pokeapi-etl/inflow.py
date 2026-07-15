def calculate_bst(stats_array):
    """Same reducer from the Python example, exposed for the CLI."""
    return sum(s.get("base_stat", 0) for s in stats_array if isinstance(s, dict))
