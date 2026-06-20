# Guardian Source — conventions & exact tokens

Distilled from the **Guardian Source Design System** (claude.ai/design project
`8c4385a2-7b49-4305-92fc-9c0ae65e75c4`), exported via DesignSync on 2026-06-20.
Every value below is copied from the source token files in this folder
(`tokens/colors.css`, `tokens/typography.css`, `tokens/spacing.css`) — the raw
files sit alongside this note and remain canonical; this is a reading aid, not a
substitute for them.

> **Substitution caveat (read before shipping).** The Guardian's real typefaces
> and the precise Source palette are reproduced here from public sources
> (`ColorPalette.swift`, the Source Figma library). Fonts are **substituted** with
> Google Fonts (see Type). The hexes are production-accurate; the fonts are not.
> For anything Guardian-branded and published, swap in the licensed families.

---

## 1. Colour — exact values

### Brand
| Token | Hex | Role |
|---|---|---|
| `--brand-400` | `#052962` | **Guardian blue** — masthead, footer, primary action |
| `--brand-100` | `#001536` | darkest blue |
| `--brand-300` | `#041f4a` | dark blue |
| `--brand-500` | `#0077b6` | mid blue — **link colour** (`--text-link`) |
| `--brand-600` | `#506991` | muted blue |
| `--brand-800` | `#c1d8fc` | pale blue tint |
| `--brand-alt-400` | `#ffe500` | **signature yellow** — reader-revenue accent, active-nav underline |
| `--brand-alt-300` | `#ffd900` | yellow |
| `--brand-alt-200` | `#f3c100` | deep yellow |

### Neutral ramp (named by lightness, 0 = black → 100 = white)
| Token | Hex | Typical use |
|---|---|---|
| `--neutral-0` | `#000000` | pure black |
| `--neutral-7` | `#121212` | **ink / primary text** |
| `--neutral-10` | `#1a1a1a` | near-black |
| `--neutral-20` | `#333333` | strong text |
| `--neutral-38` | `#606060` | — |
| `--neutral-46` | `#707070` | **secondary / muted text**, input border |
| `--neutral-60` | `#999999` | disabled text |
| `--neutral-73` | `#bababa` | — |
| `--neutral-86` | `#dcdcdc` | **hairline keyline / divider** |
| `--neutral-93` | `#ededed` | light rule |
| `--neutral-97` | `#f6f6f6` | **secondary surface** |
| `--neutral-100` | `#ffffff` | **paper / card surface** |

### Pillars (editorial wayfinding) — key steps
News is the **default** pillar. Each ramp: `100` darkest … `800` palest tint.
| Pillar | `…-400` (core) | `…-100` (dark) | `…-800` (tint) |
|---|---|---|---|
| **News** (red) | `--news-400` `#c70000` | `--news-100` `#660505` | `--news-800` `#fff4f2` |
| **Opinion** (orange) | `--opinion-400` `#c74600` | `--opinion-100` `#672005` | `--opinion-800` `#fef9f5` |
| **Sport** (blue) | `--sport-400` `#0077b6` | `--sport-100` `#003c60` | `--sport-800` `#f1f8fc` |
| **Culture** (sand) | `--culture-400` `#866d50` | `--culture-100` `#3e3323` | `--culture-800` `#fbf6ef` |
| **Lifestyle** (pink) | `--lifestyle-400` `#bb3b80` | `--lifestyle-100` `#510043` | `--lifestyle-800` `#feeef7` |

Sub-brands: **Labs** (teal) `--labs-400` `#69d1ca`; **Special report** (slate)
`--special-report-400` `#595c5f`.

### Semantic / status
| Token | Resolves to | Hex |
|---|---|---|
| `--text-success` / `--success-400` | green | `#22874d` |
| `--success-300` | dark green | `#185e36` |
| `--success-500` | light green | `#58d08b` |
| `--text-error` / `--error-400` | red | `#c70000` |
| `--error-500` | light red | `#ff9081` |
| `--focus-400` | focus blue | `#0077b6` |
| `--notification-blue-400` | notification | `#0190f7` |

> ⚠️ **`--news-400` and `--error-400` are the same hex (`#c70000`).** That's by
> design in Source, but it means "news red" and "error red" are visually
> identical. For a KPI/up-down signal, reach for the **semantic** token
> (`--text-success` / `--text-error`) and reserve news red for editorial chrome,
> so intent stays legible.

### Semantic aliases worth using directly (from `tokens/colors.css`)
`--text-primary` → `--neutral-7` · `--text-secondary` → `--neutral-46` ·
`--text-inverse` → `--neutral-100` · `--text-link` → `--brand-500` ·
`--surface-primary` → `--neutral-100` · `--surface-secondary` → `--neutral-97` ·
`--surface-inverse` → `--brand-400` · `--border-primary` → `--neutral-86` ·
`--masthead` → `--brand-400` · `--highlight` → `--brand-alt-400`.

---

## 2. Type — stacks, scale, weights

### Font-family stacks (verbatim, in order)

```css
--font-headline:   'Source Serif 4', 'GH Guardian Headline', Georgia, 'Times New Roman', serif;
--font-titlepiece: 'Source Serif 4', 'GT Guardian Titlepiece', Georgia, serif;
--font-body:       'Noto Serif', 'Guardian Text Egyptian', Georgia, serif;
--font-sans:       'Source Sans 3', 'Guardian Text Sans', system-ui, -apple-system, 'Helvetica Neue', Arial, sans-serif;
```

**Direct answer to "are Source Serif / Source Sans the fallbacks?" — No, they are
the _primary_ family, not fallbacks.** The ordering is deliberate:

1. **First = what actually renders here:** `Source Serif 4` (headline & titlepiece),
   `Noto Serif` (body), `Source Sans 3` (sans). These are the loaded Google Fonts
   substitutes.
2. **Second = the intended Guardian proprietary font**, which only takes effect if
   the licensed font is installed/served: `GH Guardian Headline`,
   `GT Guardian Titlepiece`, `Guardian Text Egyptian`, `Guardian Text Sans`.
3. **Then the true fallbacks:** `Georgia` / `Times New Roman` / generic `serif` for
   the serifs; `system-ui` → `-apple-system` → `Helvetica Neue` → `Arial` →
   `sans-serif` for the sans.

Note the body face is **Noto Serif**, *not* a Source family — Source Serif covers
the headline/titlepiece roles only. So "Source Serif / Source Sans" are the
substitutes for **headline and UI**, but the **Egyptian body** substitute is Noto
Serif. If you want to confirm against the real article face, the proprietary slab
is **Guardian Text Egyptian** (2nd in `--font-body`).

### Weights & line-heights
`--weight-regular: 400` · `--weight-medium: 500` · `--weight-bold: 700`
`--lh-tight: 1.15` (headlines) · `--lh-regular: 1.3` (UI/sans) · `--lh-loose: 1.4` (body)

### Type scale (px) — for the roles you asked about
| Role | Family | Size token → px | Weight | Line-height |
|---|---|---|---|---|
| **Headline** (front-page) | `--font-headline` (serif) | `--headline-xlarge` 34 / `--headline-large` 28 | 700 bold | 1.15 (specimens use 1.12) |
| Headline (section/feature) | `--font-headline` | `--headline-medium` 24 | 500 medium | 1.15 |
| Titlepiece (splash) | `--font-titlepiece` | `--titlepiece-small` 42 … `--titlepiece-large` 70 | 700 | 1.05–1.15, letter-spacing −0.02em |
| **Body** (reading) | `--font-body` (Egyptian) | `--body-medium` 17 / `--body-small` 15 | 400 | 1.4 |
| **Caption / label** | `--font-sans` (TextSans) | `--textsans-small` 14 | 400 (700 for labels) | 1.3 |
| **Footnote / metadata** | `--font-sans` | `--textsans-xsmall` 12 | 400 | 1.3 |

Full headline ramp: `--headline-xxxsmall` 14 · `xxsmall` 16 · `xsmall` 17 ·
`small` 20 · `medium` 24 · `large` 28 · `xlarge` 34 · `xxlarge` 42 · `xxxlarge` 50.
TextSans ramp: `xsmall` 12 · `small` 14 · `medium` 15 · `large` 17 · `xlarge` 20.

---

## 3. Spacing, shape & treatment

### Space scale (`space[n] = n × 4px`)
`--space-1` 4 · `-2` 8 · `-3` 12 · `-4` 16 · `-5` 20 · `-6` 24 · `-8` 32 ·
`-9` 36 · `-12` 48 · `-14` 56 · `-16` 64 · `-24` 96 (px).

### Border-radius — **square by default**
| Token | Value | Applies to |
|---|---|---|
| `--radius-card` | `0px` | **cards, panels — no rounding** |
| `--radius-input` | `4px` | text inputs, selects |
| `--radius-pill` | `62.5rem` | **buttons, tags, chips — fully rounded** |
| `--radius-none` | `0` | — |

### Keyline / border treatment — **borders over shadows**
| Token | Width | Use |
|---|---|---|
| `--border-hairline` | `1px` | general hairline |
| `--border-keyline` | `1px` | dividers / rules between content (colour `--neutral-86`) |
| `--border-strong` | `2px` | focus + emphasis |
| `--border-section` | `4px` | **coloured pillar rule above a section/card** |

Focus halo: `--focus-ring: 0 0 0 4px var(--brand-alt-400)` (yellow).

### Shadow — used sparingly (floating UI only)
| Token | Value |
|---|---|
| `--shadow-floating` | `0 4px 12px rgba(0,0,0,0.08)` |
| `--shadow-raised` | `0 2px 6px rgba(0,0,0,0.10)` |

Layout: 12-column, `--gutter: 20px`, `--content-max: 1300px`. Source breakpoints
`--bp-mobile` 320 → `--bp-wide` 1300.

---

## 4. Component idiom — how things are composed

**Authoring model.** Components author with **inline styles that reference the
`var(--token)` vocabulary** (see `components/buttons/Button.jsx`) rather than a
utility-class system. A set of `.gu-*` helper classes exists in
`tokens/typography.css` for foundations/specimens
(`.gu-headline`, `.gu-headline-medium`, `.gu-body`, `.gu-textsans`,
`.gu-textsans-bold`, `.gu-titlepiece`). Always reach for **semantic tokens**, not
raw hex, so a block follows whatever `data-theme` / `data-mode` scope it sits in.

**A card** — white surface, **no radius**, structure from a top keyline rather than
a box + shadow:
```html
<article style="
    background: var(--surface-primary);          /* #fff */
    border-radius: var(--radius-card);            /* 0 */
    border-top: var(--border-section) solid var(--news-400);  /* 4px pillar rule */
    padding: var(--space-4);                      /* 16px */
">
  <span class="gu-textsans-bold" style="color:var(--news-400);font-size:var(--textsans-small)">Live</span>
  <h3 class="gu-headline" style="font-size:var(--headline-medium);color:var(--text-primary)">…</h3>
</article>
```
Stack cards with `1px var(--neutral-86)` dividers between them; no gaps-as-boxes.

**A heading** — serif, bold, sentence case, tight leading:
```html
<h2 class="gu-headline" style="font-size:var(--headline-large);color:var(--text-primary)">
  Headline in sentence case
</h2>
```
(`.gu-headline` = `font-family:var(--font-headline); font-weight:700; line-height:1.15`.)

**Body text** — Egyptian slab serif, generous leading:
```html
<p class="gu-body" style="font-size:var(--body-medium);color:var(--text-primary)">…</p>
```
(`.gu-body` = `font-family:var(--font-body); font-weight:400; line-height:1.4`.)

Metadata / kickers / labels / buttons are **always TextSans** (`--font-sans`),
kickers set bold in the pillar colour above the headline.

---

## 5. Mapping onto a news-style report portal

The renderer targets a masthead, KPI cards, a numbered headline list and
drill-down links. Use these tokens:

| You need… | Token | Resolves to |
|---|---|---|
| **Masthead surface** (the deep blue bar) | `--masthead` / `--surface-inverse` | `--brand-400` `#052962` |
| Text/logo **on** the masthead | `--text-inverse` | `--neutral-100` `#ffffff` |
| **Heading colour** (serif headlines on white) | `--text-primary` | `--neutral-7` `#121212` |
| **Body text colour** | `--text-primary` | `--neutral-7` `#121212` |
| **Muted / secondary text** (timestamps, source, sub-labels) | `--text-secondary` | `--neutral-46` `#707070` |
| **Hairline border** (between list rows, card edges) | `--border-primary` | `--neutral-86` `#dcdcdc` |
| **Card background** | `--surface-primary` | `--neutral-100` `#ffffff` |
| Alt / zebra surface | `--surface-secondary` | `--neutral-97` `#f6f6f6` |
| **Positive** (KPI up, good delta) | `--text-success` | `--success-400` `#22874d` |
| **Negative** (KPI down, bad delta) | `--text-error` | `--error-400` `#c70000` |
| Drill-down **link** | `--text-link` | `--brand-500` `#0077b6` |
| Accent / highlight (active nav underline, "support" CTA) | `--highlight` | `--brand-alt-400` `#ffe500` |
| Section accent rule above a KPI card / list block | `--border-section` + pillar | `4px` + e.g. `--news-400` |

**Recipe notes for the portal**
- **Masthead:** `--brand-400` fill, white wordmark in `--font-titlepiece`, optional
  `--brand-alt-400` active-section underline. Keep it a solid bar — no shadow.
- **KPI cards:** white (`--surface-primary`), `--radius-card` (0), a `4px`
  `--border-section` top rule (pick a pillar — News red is the default editorial
  voice), figure in `--font-headline` at `--headline-large`/`xlarge`, the delta in
  `--text-success` (▲) or `--text-error` (▼), label in `--font-sans`
  `--textsans-small` `--text-secondary`. Use `--space-4`/`--space-6` padding.
- **Numbered headline list:** rows separated by `1px var(--border-primary)`, the
  rank number in `--font-headline` (or bold TextSans), the headline in
  `--font-headline` `--headline-small`/`medium` `--text-primary`, sentence case.
- **Drill-down links:** `--text-link`; on hover thicken a `--border-primary`
  underline to the full link/pillar colour (Source's link idiom) rather than
  changing size.
- **Up/down convention is an editorial choice.** Green-up/red-down is the default
  mapping above; if "up" is bad in a given metric (e.g. a price spike in a
  commodities context), swap deliberately and document it — don't let the colour
  imply a value judgement you don't mean.

---

### Source files in this folder
- `styles.css` — entry point (`@import`s the four files below)
- `tokens/colors.css` · `tokens/typography.css` · `tokens/spacing.css`
- `components/fig-tokens.css` — Figma-derived semantic tokens + `data-theme` /
  `data-mode` scopes (light/dark, pillar retint). *Contains some duplicate property
  lines — a Figma-export artifact; CSS last-value-wins resolves them.*
- `guidelines/*.card.html` — the 9 colour/type/spacing specimens
- `components/buttons/{Button.jsx, Button.prompt.md, buttons.card.html}` — idiom sample
- `readme.md` — the full system overview (voice, visual foundations, iconography)
