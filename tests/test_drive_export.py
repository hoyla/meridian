"""Tests for briefing_pack.drive_export.

Only the network-free pieces are exercised here — the upload itself needs a
live Drive/OAuth context. The socket-timeout guard is pure and is the bit
that caused real trouble (uploads hanging forever on a no-timeout socket;
2026-06-18), so it gets a regression test."""

import socket

from briefing_pack.drive_export import (
    DRIVE_SOCKET_TIMEOUT_S,
    _bounded_socket_reads,
    fix_internal_heading_links,
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


# ---------------------------------------------------------------------------
# fix_internal_heading_links — reconnecting dangling internal links to the
# real headings after a .docx → Google-Doc import. The front-page highlights
# and the Groups "Quick index" depend on this; before these tests the whole
# function was uncovered. The fake Docs service below serves one canned
# document on .get() and records every batchUpdate request body, so we can
# assert exactly which links get repointed and to which heading.
# ---------------------------------------------------------------------------

class _FakeDocsService:
    def __init__(self, document: dict):
        self._document = document
        self._pending = None
        self.batched: list[list[dict]] = []

    def documents(self):
        return self

    def get(self, documentId):  # noqa: N803 — Docs API keyword
        self._pending = None
        return self

    def batchUpdate(self, documentId, body):  # noqa: N803 — Docs API keyword
        self._pending = body["requests"]
        return self

    def execute(self):
        if self._pending is not None:
            self.batched.append(self._pending)
            self._pending = None
            return {}
        return self._document


def _heading(text: str, *, heading_id: str, named: str = "HEADING_3") -> dict:
    return {"startIndex": 0, "endIndex": 0, "paragraph": {
        "paragraphStyle": {"namedStyleType": named, "headingId": heading_id},
        "elements": [{"textRun": {"content": text}}],
    }}


def _linked_para(content: str, *, start: int, end: int, link: dict) -> dict:
    return {"paragraph": {
        "paragraphStyle": {"namedStyleType": "NORMAL_TEXT"},
        "elements": [{
            "startIndex": start, "endIndex": end,
            "textRun": {"content": content, "textStyle": {"link": link}},
        }],
    }}


def _doc(*elements: dict) -> dict:
    return {"body": {"content": list(elements)}}


def _requests(fake: _FakeDocsService) -> list[dict]:
    return [r for batch in fake.batched for r in batch]


def test_fix_links_resolves_badged_heading_by_slug():
    """The front-page case: a link whose text is the bare group name must
    resolve to the group's heading even though that heading carries a
    predictability badge (`Electric vehicles 🟢`) the link omits — both
    slugify to `electric-vehicles`. The import dropped the slug to a bare
    bookmark, so only the display text is available."""
    fake = _FakeDocsService(_doc(
        _heading("Electric vehicles 🟢", heading_id="h.ev"),
        _linked_para("Electric vehicles", start=10, end=27,
                     link={"bookmarkId": "kix.dead"}),
    ))
    assert fix_internal_heading_links(fake, "d") == 1
    upd = _requests(fake)[0]["updateTextStyle"]
    assert upd["textStyle"]["link"] == {"headingId": "h.ev"}
    assert upd["range"] == {"startIndex": 10, "endIndex": 27}


def test_fix_links_prefers_surviving_url_fragment():
    """When Google keeps the `#slug` as a url, it IS the target — resolve by
    it directly, so even a rich display text (the whole sentence) repoints
    correctly without relying on text matching."""
    fake = _FakeDocsService(_doc(
        _heading("Machine tools", heading_id="h.mt"),
        _linked_para("EU-27 exports of Machine tools to China",
                     start=5, end=44, link={"url": "#machine-tools"}),
    ))
    assert fix_internal_heading_links(fake, "d") == 1
    assert _requests(fake)[0]["updateTextStyle"]["textStyle"]["link"] == {
        "headingId": "h.mt"}


def test_fix_links_exact_text_still_resolves():
    """Regression guard: the original exact-text path (the leads digest,
    the Groups index) keeps working."""
    fake = _FakeDocsService(_doc(
        _heading("Critical minerals", heading_id="h.cm"),
        _linked_para("Critical minerals", start=3, end=20,
                     link={"bookmarkId": "kix.x"}),
    ))
    assert fix_internal_heading_links(fake, "d") == 1
    assert _requests(fake)[0]["updateTextStyle"]["textStyle"]["link"] == {
        "headingId": "h.cm"}


def test_fix_links_duplicate_slug_resolves_to_first():
    """The real findings-doc shape: a group is rendered as a badged Tier-2
    heading and again as a bare Tier-3 repeat — same slug, same target. A
    front-page link (bare group name) resolves to the FIRST occurrence (the
    richer Tier-2 block), as GitHub's `#slug` does, even though its text
    exact-matches the later bare heading."""
    fake = _FakeDocsService(_doc(
        _heading("Finished cars (broad) 🟡", heading_id="h.tier2"),
        _heading("Finished cars (broad)", heading_id="h.tier3",
                 named="HEADING_4"),
        _linked_para("Finished cars (broad)", start=2, end=23,
                     link={"bookmarkId": "kix.x"}),
    ))
    assert fix_internal_heading_links(fake, "d") == 1
    assert _requests(fake)[0]["updateTextStyle"]["textStyle"]["link"] == {
        "headingId": "h.tier2"}


def test_fix_links_leaves_external_links_alone():
    """An ordinary https link is not internal-intent and must be untouched."""
    fake = _FakeDocsService(_doc(
        _heading("Pork", heading_id="h.p"),
        _linked_para("source release", start=1, end=15,
                     link={"url": "https://example.com/x"}),
    ))
    assert fix_internal_heading_links(fake, "d") == 0
