"""Trade-gap glyph palette (Luke, 2026-09-10): Guardian blue for China's
exports and Guardian yellow for its imports, matching the masthead.

Large glyphs (China and the world) are plain fills at full strength. Small ones
(by-country dials, legend swatches, the key) edge the yellow half in Guardian
blue, compensated so the edge never inflates a half's area: the glyph encodes a
size comparison, and an SVG stroke straddles its path."""
import re

from report_render_html import (
    _FLOW_EXPORT, _FLOW_IMPORT, _SMALL_EDGE_PX,
    _balance_glyph_svg, _gacc_world_bubbles_svg, _half_disc,
)

_ROWS = [
    {"kind": "bloc", "label": "ASEAN", "short_label": "ASEAN", "hub": None,
     "flows": {"export": {"rolling_eur": 673.3e9}, "import": {"rolling_eur": 392.0e9}}},
    {"kind": "country", "label": "Russian Federation", "short_label": "Russia", "hub": None,
     "flows": {"export": {"rolling_eur": 106.4e9}, "import": {"rolling_eur": 125.2e9}}},
]


def _world():
    return _gacc_world_bubbles_svg(_ROWS, "2026-08-01")


def test_palette_is_guardian_blue_and_yellow():
    assert _FLOW_EXPORT == "#052962"   # brand-400, the masthead blue
    assert _FLOW_IMPORT == "#ffe500"   # brand-alt-400, the masthead yellow


def test_small_glyph_edge_does_not_inflate_the_half():
    """The stroke's OUTER edge must land on the true radius. Hong Kong-like
    flows give the smallest real sliver, where inflation bites hardest."""
    exp, imp = 380.2e9, 45.8e9
    svg = _balance_glyph_svg(exp, imp)
    m = re.search(r'a([\d.]+) [\d.]+ 0 0 1 0 [\d.]+z" fill="#ffe500" '
                  r'stroke="#052962" stroke-width="([\d.]+)"', svg)
    assert m, svg
    r_draw, sw = float(m.group(1)), float(m.group(2))
    assert abs((r_draw + sw / 2) - 9.0 * (imp / exp) ** 0.5) < 0.01
    assert abs(sw - _SMALL_EDGE_PX) < 1e-9
    assert 'fill="#052962"/>' in svg           # export half: navy, unedged


def test_half_too_small_to_edge_is_drawn_plain_not_shrunk_away():
    out = _half_disc(10, 10, 0.3, 1, "#ffe500", 0.8)
    assert "stroke=" not in out and "a0.30 0.30" in out


def test_large_world_glyphs_are_unedged_at_full_strength():
    out = _world()
    svg = out[:out.index("</svg>")]            # the strip itself, not the legend
    assert 'fill="#052962">' in svg and 'fill="#ffe500">' in svg
    assert "fill-opacity" not in svg
    assert 'stroke="#052962"' not in svg        # no edge at the large size


def test_world_legend_uses_edged_swatches_not_coloured_characters():
    """Yellow TEXT on white cannot be edged, so the legend draws small
    half-disc swatches, and the import one carries the edge."""
    cap = _world()
    cap = cap[cap.index("gchart-caption"):]
    assert "◖" not in cap and "◗" not in cap
    assert 'fill="#ffe500" stroke="#052962"' in cap
