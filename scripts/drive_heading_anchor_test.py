"""Heading-anchor decision spike — OAuth + Drive upload + Docs-API touch.

Settles the open question from the docx-anchor investigation (see
`dev_notes/2026-05-16_docx-drive-spike.md`): Google Docs assigns a
`headingId` (the `#heading=h.xxxx` navigation anchor) lazily, when a
heading paragraph is created/edited *through the editor*. Markdown
import mints them; .docx import does not. Manually cutting+pasting a
single heading mints it; a bulk select-all paste does NOT.

The deciding question for the production delivery path is therefore:
**does a programmatic Docs-API edit count as the kind of per-paragraph
edit that mints a headingId?**

  - If YES → keep generating the .docx exactly as now, upload with
    conversion, then run one batched "touch every heading" pass. Charts
    ride along in the conversion for free; least-work path.
  - If NO  → fall back to converting the canonical .md (native headings
    that arrive WITH anchors) and re-inserting charts via the Docs API
    by index; more work, but anchor-correct by construction.

This script tests two candidate touches on two separate headings and
reports which (if any) produces a headingId:

  Touch A — content edit: insert a throwaway char into the heading then
            delete it (a no-op that still "edits" the paragraph).
  Touch B — style re-apply: updateParagraphStyle re-applying the SAME
            namedStyleType over the heading's range.

Nothing is destroyed: the uploaded test Doc is left in Drive so the URL
fragment behaviour can be eyeballed by hand as a cross-check.

Usage:
    ./.venv/bin/python scripts/drive_heading_anchor_test.py

First run opens a browser for OAuth consent (scope: drive.file only);
the refresh token persists to ~/.config/meridian/google-token.json so
subsequent runs are silent. No DB access required — operates purely on
the already-generated test .docx plus the Drive/Docs APIs.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# drive.file: per-file access to files the app creates/opens. This same
# scope lets the app both CREATE the Doc (Drive upload-conversion) and
# EDIT it afterwards (Docs API), because the app owns the file — so we
# avoid the broader, sensitive `documents`/`drive` scopes entirely.
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

CONFIG_DIR = Path(os.path.expanduser("~/.config/meridian"))
CLIENT_SECRET = CONFIG_DIR / "client_secret.json"
TOKEN = CONFIG_DIR / "google-token.json"

TEST_DOCX = Path("exports/test-heading-fix-2026-05-21/03_Findings.docx")

DOCX_MIME = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)
GDOC_MIME = "application/vnd.google-apps.document"


# ---------------------------------------------------------------------------
# OAuth
# ---------------------------------------------------------------------------

def get_credentials() -> Credentials:
    """InstalledAppFlow with token persistence. First run prompts for
    browser consent; later runs read (and silently refresh) the token."""
    creds: Credentials | None = None
    if TOKEN.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CLIENT_SECRET.exists():
                sys.exit(
                    f"Missing OAuth client secret at {CLIENT_SECRET}. "
                    "Download it from the GCP console (Desktop-app client) "
                    "and save it there."
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CLIENT_SECRET), SCOPES,
            )
            creds = flow.run_local_server(port=0)
        TOKEN.write_text(creds.to_json())
        os.chmod(TOKEN, 0o600)
    return creds


# ---------------------------------------------------------------------------
# Drive upload (with conversion to a native Google Doc)
# ---------------------------------------------------------------------------

def upload_docx_as_doc(drive, local_path: Path, name: str) -> tuple[str, str]:
    """Upload `local_path` (.docx) to Drive, converting to a Google Doc.
    Returns (document_id, web_view_link)."""
    media = MediaFileUpload(str(local_path), mimetype=DOCX_MIME, resumable=True)
    f = drive.files().create(
        body={"name": name, "mimeType": GDOC_MIME},
        media_body=media,
        fields="id,webViewLink",
    ).execute()
    return f["id"], f.get("webViewLink", "")


# ---------------------------------------------------------------------------
# Docs API — read headings + apply touches
# ---------------------------------------------------------------------------

def list_headings(docs, doc_id: str) -> list[dict]:
    """Return one dict per heading paragraph: start/end index, the
    namedStyleType, the headingId (or None), and the heading text."""
    doc = docs.documents().get(documentId=doc_id).execute()
    out: list[dict] = []
    for el in doc.get("body", {}).get("content", []):
        para = el.get("paragraph")
        if not para:
            continue
        style = para.get("paragraphStyle", {})
        named = style.get("namedStyleType", "")
        if not named.startswith("HEADING_"):
            continue
        text = "".join(
            e.get("textRun", {}).get("content", "")
            for e in para.get("elements", [])
        ).strip()
        out.append({
            "start_index": el.get("startIndex"),
            "end_index": el.get("endIndex"),
            "named_style": named,
            "heading_id": style.get("headingId"),
            "text": text,
        })
    return out


def _resolve(docs, doc_id: str, text: str) -> dict | None:
    """Re-read the doc and return the current heading dict whose text
    matches `text`. Called immediately before each touch so indices are
    always live (touches earlier in the run may have shifted them)."""
    for h in list_headings(docs, doc_id):
        if h["text"] == text:
            return h
    return None


def _set_style(docs, doc_id: str, start: int, end: int, named: str) -> None:
    docs.documents().batchUpdate(
        documentId=doc_id,
        body={"requests": [
            {"updateParagraphStyle": {
                "range": {"startIndex": start, "endIndex": end},
                "paragraphStyle": {"namedStyleType": named},
                "fields": "namedStyleType",
            }},
        ]},
    ).execute()


def touch_content_edit(docs, doc_id: str, h: dict) -> None:
    """Touch A — insert a throwaway char at the heading start, then
    delete it. Two separate batchUpdate calls so it reads as two genuine
    edits rather than one coalesced no-op."""
    s = h["start_index"]
    docs.documents().batchUpdate(
        documentId=doc_id,
        body={"requests": [
            {"insertText": {"location": {"index": s}, "text": "x"}},
        ]},
    ).execute()
    docs.documents().batchUpdate(
        documentId=doc_id,
        body={"requests": [
            {"deleteContentRange": {"range": {
                "startIndex": s, "endIndex": s + 1,
            }}},
        ]},
    ).execute()


def touch_style_reapply(docs, doc_id: str, h: dict) -> None:
    """Touch B — re-apply the SAME namedStyleType (likely a no-op to
    Google; kept as the control)."""
    _set_style(docs, doc_id, h["start_index"], h["end_index"], h["named_style"])


def touch_style_flip(docs, doc_id: str, h: dict) -> None:
    """Touch C — a REAL style change: NORMAL_TEXT, then back to the
    original heading style. Two separate edits."""
    _set_style(docs, doc_id, h["start_index"], h["end_index"], "NORMAL_TEXT")
    h2 = _resolve(docs, doc_id, h["text"]) or h  # indices unchanged, but be safe
    _set_style(docs, doc_id, h2["start_index"], h2["end_index"], h["named_style"])


def touch_recreate(docs, doc_id: str, h: dict) -> None:
    """Touch D — delete the heading paragraph and re-insert it as a fresh
    paragraph, then style it. The closest API analogue to the manual
    cut/paste that is known to mint anchors. Net-zero length overall."""
    s, e = h["start_index"], h["end_index"]
    text = h["text"]
    docs.documents().batchUpdate(
        documentId=doc_id,
        body={"requests": [
            {"deleteContentRange": {"range": {"startIndex": s, "endIndex": e}}},
        ]},
    ).execute()
    docs.documents().batchUpdate(
        documentId=doc_id,
        body={"requests": [
            {"insertText": {"location": {"index": s}, "text": text + "\n"}},
        ]},
    ).execute()
    _set_style(docs, doc_id, s, s + len(text) + 1, h["named_style"])


TOUCHES = [
    ("A content-edit", touch_content_edit),
    ("B style-reapply", touch_style_reapply),
    ("C style-flip", touch_style_flip),
    ("D recreate", touch_recreate),
]


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    if not TEST_DOCX.exists():
        sys.exit(
            f"Test docx not found at {TEST_DOCX}. Generate it first, e.g.:\n"
            "  set -a; . ./.env; set +a; ./.venv/bin/python -c \"import "
            "briefing_pack; briefing_pack.export("
            "out_dir='./exports/test-heading-fix-2026-05-21', docx=True, "
            "spreadsheet=False, record=False)\""
        )

    creds = get_credentials()
    drive = build("drive", "v3", credentials=creds)
    docs = build("docs", "v1", credentials=creds)

    name = f"meridian-anchor-test-{datetime.now():%Y%m%d-%H%M%S}"
    print(f"Uploading {TEST_DOCX} as Google Doc '{name}' ...")
    doc_id, link = upload_docx_as_doc(drive, TEST_DOCX, name)
    print(f"  document_id: {doc_id}")
    print(f"  open it:     {link}\n")

    headings = list_headings(docs, doc_id)
    with_id = [h for h in headings if h["heading_id"]]
    without_id = [h for h in headings if not h["heading_id"]]
    print(f"BASELINE after import: {len(headings)} headings; "
          f"{len(with_id)} have a headingId, {len(without_id)} do not.")
    if not without_id:
        print("\nAll imported headings already carry anchors — no touch "
              "needed. (Unexpected given prior observation; verify in the "
              "browser.)")
        return

    # One distinct, UNIQUELY-named anchor-less heading per touch, so we
    # can match it back unambiguously after the run.
    seen: dict[str, int] = {}
    for h in headings:
        seen[h["text"]] = seen.get(h["text"], 0) + 1
    pool = [
        h for h in without_id
        if seen[h["text"]] == 1 and h["text"]
    ]
    if len(pool) < len(TOUCHES):
        sys.exit(f"Need {len(TOUCHES)} uniquely-named headings to test; "
                 f"only {len(pool)} available.")
    chosen = list(zip(TOUCHES, pool[:len(TOUCHES)]))

    for (label, fn), target in chosen:
        live = _resolve(docs, doc_id, target["text"])  # fresh indices
        if live is None:
            print(f"  ! {label}: target vanished, skipping")
            continue
        print(f"Touch {label} on @ {live['start_index']}: "
              f"{live['text'][:50]!r}")
        fn(docs, doc_id, live)

    after = {h["text"]: h["heading_id"] for h in list_headings(docs, doc_id)}

    print("\n=== RESULT ===")
    any_minted = False
    winners: list[str] = []
    for (label, _), target in chosen:
        hid = after.get(target["text"])
        minted = bool(hid)
        any_minted = any_minted or minted
        if minted:
            winners.append(label)
        print(f"Touch {label:16}: headingId now = {hid!r}  "
              f"-> {'MINTED' if minted else 'still none'}")

    print()
    if any_minted:
        print(f"VERDICT: programmatic edit(s) DID mint anchors via: "
              f"{', '.join(winners)}.")
        print("=> docx-upload + a batched pass of the winning touch over all "
              "headings is viable. Charts ride along in the conversion for "
              "free — this is the least-work path.")
    else:
        print("VERDICT: none of the four touches minted an anchor.")
        print("=> the docx-import path can't be rescued by an API touch; "
              "use the markdown-convert path (native anchors) + Docs-API "
              "chart insertion for anchor-correct delivery.")
    print(f"\nCross-check by hand in the browser: {link}")


if __name__ == "__main__":
    main()
