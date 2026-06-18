"""Tests for briefing_pack.drive_export.

Only the network-free pieces are exercised here — the upload itself needs a
live Drive/OAuth context. The socket-timeout guard is pure and is the bit
that caused real trouble (uploads hanging forever on a no-timeout socket;
2026-06-18), so it gets a regression test."""

import socket

from briefing_pack.drive_export import (
    DRIVE_SOCKET_TIMEOUT_S,
    _bounded_socket_reads,
)


def test_bounded_socket_reads_sets_and_restores():
    """The context manager sets the default socket timeout for the block and
    restores the previous value on exit — including when the block raises."""
    prev = socket.getdefaulttimeout()
    try:
        socket.setdefaulttimeout(None)  # the real-world starting state
        with _bounded_socket_reads():
            assert socket.getdefaulttimeout() == DRIVE_SOCKET_TIMEOUT_S
        assert socket.getdefaulttimeout() is None

        # A custom value, and restoration even on exception.
        socket.setdefaulttimeout(7.0)
        try:
            with _bounded_socket_reads(30):
                assert socket.getdefaulttimeout() == 30
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        assert socket.getdefaulttimeout() == 7.0
    finally:
        socket.setdefaulttimeout(prev)
