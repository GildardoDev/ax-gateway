from ax_cli.mentions import extract_explicit_mentions, merge_explicit_mentions_metadata


def test_extract_explicit_mentions_dedupes_and_excludes_self():
    assert extract_explicit_mentions(
        "@nemotron please ask @Hermes and @nemotron again, not email@example.com",
        exclude=["hermes"],
    ) == ["nemotron"]


def test_merge_explicit_mentions_metadata_preserves_existing_values():
    metadata = {"routing": {"mode": "reply_target"}, "mentions": ["existing"]}

    merged = merge_explicit_mentions_metadata(metadata, "@nemotron ping @existing")

    assert merged == {
        "routing": {"mode": "reply_target"},
        "mentions": ["existing", "nemotron"],
    }
    assert metadata["mentions"] == ["existing"]


def test_extract_skips_mentions_inside_fenced_code_blocks():
    content = (
        "Heads up @real_agent, here's the fix:\n"
        "```python\n"
        "@decorator\n"
        "def handler(): pass\n"
        "```\n"
        "let me know."
    )
    assert extract_explicit_mentions(content) == ["real_agent"]


def test_extract_skips_mentions_inside_inline_code():
    assert extract_explicit_mentions("see `@param` docs and ping @real_agent") == ["real_agent"]


def test_extract_skips_npm_scoped_package_names():
    # @types/node is a path-like reference, not an aX handle. The trailing
    # slash lookahead pairs with the existing leading-slash lookbehind.
    assert extract_explicit_mentions("install @types/node and ping @real_agent") == ["real_agent"]


def test_extract_handles_unclosed_fence_conservatively():
    # An unclosed ``` fence drops everything from the fence to end-of-content.
    # Better to miss a real mention than to leak a fake one from inside code.
    content = "context ```unclosed\n@decorator and @real_agent never extracted"
    assert extract_explicit_mentions(content) == []


def test_extract_keeps_mention_when_fence_closes_before_it():
    content = (
        "```python\n"
        "@decorator\n"
        "```\n"
        "@real_agent please review"
    )
    assert extract_explicit_mentions(content) == ["real_agent"]


def test_merge_metadata_drops_code_block_mentions():
    metadata = {"routing": {"mode": "reply_target"}}
    content = (
        "Pinging @nemotron — here's the snippet:\n"
        "```python\n"
        "@decorator\n"
        "```\n"
    )
    merged = merge_explicit_mentions_metadata(metadata, content)
    assert merged == {"routing": {"mode": "reply_target"}, "mentions": ["nemotron"]}
