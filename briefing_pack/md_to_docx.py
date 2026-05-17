"""Markdown → docx translator for the briefing pack.

Walks a mistune AST of a rendered findings.md document and emits the
equivalent shape into a python-docx Document. Charts can be injected
at specific anchor points (currently after each top-mover list item)
via a callable passed in at construction.

Why not pandoc:
- meridian is pure-Python with no subprocess dependencies; pandoc
  would add an external binary requirement.
- Our markdown subset is small (~10 block types, ~5 inline types).
  A focused translator stays under 400 lines and gives full output
  control.

Why this exists as a separate module from `briefing_pack.docx`:
- `briefing_pack.docx` orchestrates: page setup, top-movers chart
  data fetch, final write.
- `briefing_pack.md_to_docx` is a pure translator: in = markdown
  string, out = blocks written to a Document. Reusable for any
  future docx-from-markdown surface (leads.docx, groups.docx, etc.).

The mistune AST node types we handle (and what we don't):

Handled:
- heading (levels 1-6)
- paragraph
- list (ordered + unordered, including nested)
- list_item, block_text
- table, table_head, table_body, table_row, table_cell
- block_quote
- block_code (fenced)
- thematic_break (horizontal rule)
- text, strong, emphasis, codespan, link
- softbreak, linebreak

Not handled / passed through as plain text:
- HTML blocks / inline HTML (none emitted by our sections today)
- Footnotes, definition lists, task lists (not in our markdown subset)
- Images (sections emit none; provenance bundle is link-only)
"""

from __future__ import annotations

import logging
import re
from typing import Callable

import mistune
from docx import Document
from docx.shared import Pt

log = logging.getLogger(__name__)


# Finding-token regex shared with briefing_pack/render.py's
# _editorially_fresh_finding_ids. Lifted here so the translator can
# detect chart-injection anchors in list items.
_FINDING_TOKEN_RE = re.compile(r"\bfinding/(\d+)\b")


# Type alias: callable that takes a finding id and returns PNG bytes
# (or None if no chart is available for that finding).
ChartLookup = Callable[[int], bytes | None]


class MarkdownToDocxTranslator:
    """Walk a mistune AST and write each block into a python-docx
    Document. Inline runs (bold / italic / code / link) become run
    objects with the right styling inside the appropriate block.

    Chart injection: when `chart_for_finding` is provided, after each
    list item whose text carries a `finding/{id}` token, the
    translator inserts a chart picture if `chart_for_finding(id)`
    returns PNG bytes.

    Stateless apart from the target Document and the chart-lookup
    callable. Safe to instantiate per-render.
    """

    def __init__(
        self,
        doc: Document,
        *,
        chart_for_finding: ChartLookup | None = None,
        chart_width_mm: int = 190,
    ) -> None:
        self.doc = doc
        self.chart_for_finding = chart_for_finding
        self.chart_width_mm = chart_width_mm
        # mistune parser with table support (the briefing pack's tables
        # use the GFM pipe syntax).
        self._parse = mistune.create_markdown(
            renderer=None, plugins=["table", "strikethrough"],
        )

    # ----- public entry point ------------------------------------------------

    def translate(self, markdown_text: str) -> None:
        """Translate `markdown_text` into the underlying Document.

        Iterates the top-level AST nodes; each block emits one or more
        docx blocks. Existing Document content is preserved (the
        translator appends).
        """
        ast = self._parse(markdown_text)
        if not isinstance(ast, list):
            # Defensive — mistune always returns a list for the top level,
            # but renderer=None can return other shapes in edge cases.
            log.warning("Unexpected AST root: %r — skipping", type(ast))
            return
        for node in ast:
            self._handle_block(node)

    # ----- block dispatch ----------------------------------------------------

    def _handle_block(self, node: dict) -> None:
        t = node.get("type", "")
        handler = getattr(self, f"_block_{t}", None)
        if handler is None:
            # Unknown block — pass through as plain paragraph if we can
            # extract any text. Real-world cases: future markdown features
            # we haven't taught the translator yet.
            text = self._collect_plain_text(node)
            if text.strip():
                self.doc.add_paragraph(text)
            return
        handler(node)

    # Stubs filled in by subsequent commits.
    def _block_blank_line(self, node: dict) -> None:
        # mistune emits these between blocks; python-docx handles
        # paragraph spacing via styles, so we drop them.
        pass

    def _collect_plain_text(self, node: dict) -> str:
        """Recursively concatenate `raw` from every text-like descendant.
        Used as a fallback for unknown block types and for the
        finding-token scan in list items."""
        if "raw" in node and "children" not in node:
            return node.get("raw") or ""
        parts: list[str] = []
        for child in node.get("children") or []:
            parts.append(self._collect_plain_text(child))
        return "".join(parts)
