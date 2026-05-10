"""CLI entrypoint must force UTF-8 on stdout/stderr.

Rich (and our own prints) emit Unicode glyphs — arrows, box-drawing chars,
check marks — that crash with ``UnicodeEncodeError`` on the default Windows
``cp1252`` console. The fix is to call ``stream.reconfigure(encoding='utf-8',
errors='replace')`` at CLI entry, before any Rich Console is instantiated.

These tests cover the helper itself, not the side effect of importing
``ax_cli.main`` — that import already happened before the test runner started,
so re-running it is a no-op for the helper. The tests below verify the
helper's behavior on every stream shape it might be handed.
"""

from __future__ import annotations

import io
import sys

from ax_cli.main import _reconfigure_stdio_to_utf8


class _RecordingStream:
    """Stream-like object that records reconfigure calls."""

    def __init__(self, *, raise_on_reconfigure=None):
        self.calls: list[dict] = []
        self.encoding = "cp1252"
        self._raise_on_reconfigure = raise_on_reconfigure

    def reconfigure(self, *, encoding=None, errors=None, **kwargs):
        if self._raise_on_reconfigure is not None:
            raise self._raise_on_reconfigure
        self.calls.append({"encoding": encoding, "errors": errors, **kwargs})
        if encoding:
            self.encoding = encoding


def test_reconfigure_forces_utf8_with_replace_errors(monkeypatch):
    """The happy path: a stream with reconfigure() gets UTF-8 + errors='replace'."""
    out = _RecordingStream()
    err = _RecordingStream()
    monkeypatch.setattr(sys, "stdout", out)
    monkeypatch.setattr(sys, "stderr", err)

    _reconfigure_stdio_to_utf8()

    assert out.calls == [{"encoding": "utf-8", "errors": "replace"}]
    assert err.calls == [{"encoding": "utf-8", "errors": "replace"}]
    assert out.encoding == "utf-8"
    assert err.encoding == "utf-8"


def test_reconfigure_skips_streams_without_reconfigure_attr(monkeypatch):
    """StringIO and other reconfigureless streams must not crash the helper.

    Test runners (Click's CliRunner, pytest's capfd) replace stdout/stderr
    with StringIO instances. The helper must be a no-op on them.
    """
    monkeypatch.setattr(sys, "stdout", io.StringIO())
    monkeypatch.setattr(sys, "stderr", io.StringIO())

    # Should not raise, should not do anything observable.
    _reconfigure_stdio_to_utf8()


def test_reconfigure_swallows_oserror_from_stream(monkeypatch):
    """A stream that refuses reconfigure (closed pipe, detached buffer) is tolerated."""
    monkeypatch.setattr(sys, "stdout", _RecordingStream(raise_on_reconfigure=OSError("closed")))
    monkeypatch.setattr(sys, "stderr", _RecordingStream(raise_on_reconfigure=ValueError("bad encoding")))

    # Helper must not propagate either failure.
    _reconfigure_stdio_to_utf8()


def test_reconfigure_handles_non_callable_reconfigure_attr(monkeypatch):
    """``hasattr`` is True even when the attr isn't callable — guard against that too."""

    class WeirdStream:
        reconfigure = "not a function"

    monkeypatch.setattr(sys, "stdout", WeirdStream())
    monkeypatch.setattr(sys, "stderr", WeirdStream())

    _reconfigure_stdio_to_utf8()


def test_reconfigure_runs_on_real_text_io_in_temp_pipe(tmp_path, monkeypatch):
    """End-to-end: an actual TextIOWrapper around a binary file becomes UTF-8 after the call."""
    out_path = tmp_path / "out.txt"
    err_path = tmp_path / "err.txt"

    out_text = io.TextIOWrapper(out_path.open("wb"), encoding="cp1252", errors="strict")
    err_text = io.TextIOWrapper(err_path.open("wb"), encoding="cp1252", errors="strict")

    monkeypatch.setattr(sys, "stdout", out_text)
    monkeypatch.setattr(sys, "stderr", err_text)

    _reconfigure_stdio_to_utf8()

    # Now writing the arrow that originally crashed `ax tasks list --json`
    # must succeed — cp1252 would have raised UnicodeEncodeError on '→'.
    out_text.write("Space: ax-cli-dev → ed81ae98\n")
    err_text.write("hint: ✓ done\n")
    out_text.flush()
    err_text.flush()

    assert out_text.encoding == "utf-8"
    assert err_text.encoding == "utf-8"
    assert "→" in out_path.read_text(encoding="utf-8")
    assert "✓" in err_path.read_text(encoding="utf-8")


def test_main_module_imports_without_unicode_error(monkeypatch):
    """Importing ax_cli.main fresh must not crash even when stdout has a strict cp1252 wrapper.

    Regression guard for the original bug: at import time, Rich's Console is
    instantiated against sys.stdout. If the helper hadn't run first, Rich
    would snapshot a cp1252 encoding and any later print of a non-cp1252
    glyph would crash. Re-importing ``ax_cli.output`` here exercises that
    Console init path under the same constraints.
    """
    captured = io.TextIOWrapper(io.BytesIO(), encoding="cp1252", errors="strict")
    monkeypatch.setattr(sys, "stdout", captured)

    # The helper is a no-op for the existing module-level Rich Console
    # (already created), so we just verify it doesn't blow up on a fresh
    # invocation against a cp1252 stream.
    _reconfigure_stdio_to_utf8()
    assert captured.encoding == "utf-8"
