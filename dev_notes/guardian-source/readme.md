# The Guardian — Source Design System

A working design system for **The Guardian**, recreated from the Guardian's own
**Source** design language. It contains the brand foundations (colour, type, spacing),
the real Guardian wordmark and roundel, a UI-icon set, reusable React components, and
two product UI kits (the website front page and the iOS news app).

*Current inventory: **540 tokens** (registered across 6 theme/pillar scopes), **153 UI
icons** + brand glyphs, **61 bundled components** (15 hand-authored primitives + 46
materialised brand/icon components), **19 specimen/kit cards** across 7 groups, **2
copy-ready templates**, and **2 product UI kits**.*

> **The Guardian** is a British daily newspaper and one of the world's largest
> liberal news organisations, publishing free-to-read journalism funded primarily by
> reader contributions. Its products span **theguardian.com**, native **iOS/Android
> apps**, the **Editions** app, email newsletters, podcasts and Guardian Labs.
> "Source" is the Guardian's public, open design system.

## Sources used to build this system
- **Figma:** "◈ Core library (Copy)" — the Guardian Source Figma library (foundations,
  brand assets, icons and the full component set). Tokens, the wordmark/roundel and the
  UI icons in this project were materialised directly from it.
- **GitHub — [guardian/source-apps](https://github.com/guardian/source-apps):** the
  Swift/Kotlin Source library for the native apps; the exact colour palette
  (`ColorPalette.swift`) was lifted from here.
- Related public repos worth exploring for deeper fidelity:
  [guardian/source](https://github.com/guardian/source) (web component library),
  [guardian/dotcom-rendering](https://github.com/guardian/dotcom-rendering) (the live
  website), and [guardian/fonts](https://github.com/guardian/fonts).
- Public reference: **https://theguardian.design** and **https://design.theguardian.com**.

*The reader is not assumed to have access to the Figma file; everything needed is
captured in this project. The links are kept so anyone who does have access can go deeper.*

---

## CONTENT FUNDAMENTALS — how the Guardian writes

**Voice.** Plain, precise, serious but human. The Guardian writes in clear British
English, favouring active verbs and concrete nouns over jargon. Headlines state, they
don't tease — *"NHS waiting lists fall for third month running, figures show"*, not
*"You won't believe what just happened to the NHS"*.

- **Person & address.** Editorial copy is third-person and reported. The brand speaks as
  *"the Guardian"* / *"we"* only in reader-revenue and institutional moments
  (*"… we have a small favour to ask"*, *"Support the Guardian"*). It addresses the
  reader as *"you"* in those CTAs and in product UI ("Sign in", "Save for later").
- **Casing.** **Sentence case almost everywhere** — headlines, buttons, nav, section
  headers. Not title case. The masthead wordmark is the famous lower-case *"theguardian"*
  lockup; the product wordmark renders *"The Guardian"*.
- **Kickers & labels.** Short, bold, in the pillar colour, above the headline:
  *Live, Exclusive, Analysis, Opinion, Film, Football*. A live story shows a pulsing red dot.
- **Bylines.** Set in italic headline serif — *"George Monbiot"*, *"Marina Hyde,
  Environment editor"*.
- **Tone by pillar.** News is sober and factual; Opinion is named, personal and
  argumentative; Sport is energetic; Culture is critical (the star rating is editorial
  shorthand); Lifestyle is warm and service-led.
- **Reader revenue.** The signature ask is understated and earnest, never pushy:
  *"Millions turn to the Guardian for open, independent journalism every day. Will you
  make it three?"* paired with a yellow **Support the Guardian** button.
- **Emoji:** not used in editorial or product chrome. Iconography does the work instead.

---

## VISUAL FOUNDATIONS

**Overall feel.** Confident broadsheet authority translated to screen: a deep blue
masthead, crisp white reading surfaces, serif headlines, and editorial colour used as
a *wayfinding system* (the five pillars) rather than decoration. Structure comes from
**keylines, not boxes** — the Guardian draws hairline rules between and above content
instead of wrapping it in cards and shadows.

### Colour
- **Guardian blue `#052962`** (brand-400) is the masthead, footer and primary action
  colour. **Yellow `#FFE500`** (brand-alt-400) is the reader-revenue accent — used for
  the *Support us* button and the active-nav underline, sparingly and with intent.
- **Five editorial pillars**, each a full tint ramp: **News** red, **Opinion** orange,
  **Sport** blue, **Culture** sand, **Lifestyle** pink. Plus sub-brands **Labs** (teal,
  sponsored) and **Special report** (slate, investigations).
- A long **neutral ramp** (named by lightness 0→100) does the structural work: `neutral-7`
  ink text, `neutral-86` keylines, `neutral-97` secondary surfaces, `neutral-100` paper.
- **Runtime theming.** Colour is not baked in — semantic tokens re-resolve under scope
  attributes set on `:root` (defined in `components/fig-tokens.css`):
  - **Appearance:** `data-theme="light"` / `data-theme="dark"` (also `.light` / `.dark`
    class hooks) swap the background/ink/input token sets for light- and dark-mode
    reading surfaces.
  - **Pillar mode:** `data-mode="opinion" | "sport" | "culture" | "lifestyle"` retints the
    global background and accent tokens to that pillar's ramp; **News is the default**
    (no `data-mode` needed). Set the attribute on a section wrapper and the components
    inside inherit the pillar's colourway.
  Author with the semantic tokens (not raw hexes) so a design follows whichever
  appearance + pillar scope it is dropped into.
- Imagery is full-colour documentary photography (warm, true-to-life — not filtered or
  duotoned, except special-report/opinion treatments). Placeholders here are flat grey.

### Type
- Three families (proprietary; **substituted** here — see caveat): **GH Guardian
  Headline** (display serif, headlines), **Guardian Text Egyptian** (slab serif, article
  body), **Guardian Text Sans** (humanist sans, UI/metadata), plus **GT Guardian
  Titlepiece** for splash moments.
- Headlines are serif and **bold**, set tight (line-height ~1.15) in sentence case. Body
  reading text is the slab serif at 15–17px with generous 1.4 line-height. All chrome,
  labels, buttons and metadata are TextSans.

### Space, shape & layout
- **4px base unit** (`space[n] = n × 4`). A 12-column, 20px-gutter grid at desktop with
  Source breakpoints (320 → 1300px).
- **Square by default.** Cards have **no radius**; inputs get a 4px hairline radius;
  **buttons and tags are fully rounded pills**. The look is sharp, not soft.
- Borders over shadows: 1px `neutral-86` dividers, **4px coloured pillar rules** above
  section content and cards. Shadows are reserved for genuinely floating UI (menus,
  toasts) — `0 4px 12px rgba(0,0,0,.08)`.

### Motion, hover & press
- Restrained and functional — short (120–150ms) ease transitions on colour and border.
  No bounce, no parallax, no decorative loops.
- **Hover:** links thicken their keyline underline to the full pillar colour; buttons
  darken their fill slightly. **Press:** colour shift, not scale. **Focus:** a
  Guardian-blue inner keyline / yellow halo on form controls.
- The only "animation" of note is the **pulsing red Live dot**.

### Transparency & blur
- Used sparingly: translucent black media pills over photography (`rgba(0,0,0,.7)`),
  and iOS "liquid glass" chrome in the app frame. Reading surfaces stay opaque.

---

## ICONOGRAPHY
- The Guardian uses a bespoke **single-weight, 24×24 outlined icon set** (the Source
  icons). Icons are monochrome and paint with `currentColor`, so they recolour to white
  on the blue chrome and ink on white surfaces.
- This project **copies the real Source icons** out of the Figma library as standalone
  SVGs in `assets/icons/svg/` — **the full 153-icon UI set** at 24×24: arrows &
  chevrons, alerts (`alert-round`, `tick-round`, `info-round`, `exclamation`), `menu`,
  `settings`, `filter`, `reload`, `ellipsis`, search (`magnifying-glass` ±), people
  (`person`, `person-plus/cross/tick`), `speech-bubble` ±, media controls
  (`media-controls-play/pause/stop/forward/back`, `audio`, `video`, `camera`), `share`,
  `bookmark`, `folder`, `gift`, `quote`, `clock`, `home` variants, `discover`, recipe
  icons, payment glyphs (`credit-card`, `direct-debit`, `pay-pal`) and social brands
  (`facebook`, `twitter`, `whats-app`, `telegram`, `linked-in`, `pinterest`, `apple`,
  `google`, `signal`). They paint with `currentColor`. The materialised PascalCase `.jsx`
  versions of the 40 most-used ones sit alongside in `assets/icons/`.
- **No emoji** and **no icon font** — SVG only. Brand glyphs (the wordmark, the G
  roundel) are vector assets in `assets/brand/`, also `currentColor`-driven.
- Need an icon that isn't here? Match the set's stroke weight and 24px grid, or
  materialise more from the Figma `Icons/UI-icons-24x24` page rather than drawing by hand.

---

## INDEX — what's in this project

**Foundations**
- `styles.css` — global entry point (import this one file). `@import`s everything below.
- `tokens/colors.css` · `tokens/typography.css` · `tokens/spacing.css` — the token layer.
- `components/fig-tokens.css` — Figma-derived semantic tokens + theme/pillar modes
  (`data-theme="light|dark"`, `data-mode="opinion|sport|culture|lifestyle"`).
- `@dsCard`-tagged `.html` files — 19 specimen/kit cards across 7 groups, co-located with
  what they document:
  - `guidelines/*.card.html` (9) — **Type** (Headline & Titlepiece, Body — Egyptian,
    TextSans), **Colors** (Brand, Neutral ramp, Pillars, Utility & sub-brands) and
    **Spacing** (Space scale, Radius/keylines/shadow).
  - `components/*/*.card.html` (6) — **Components**: Button, Choice card, Editorial,
    Feedback, Form controls, Table.
  - `assets/*/*.card.html` (2) — **Brand**: Logo & roundel, UI icon library.
  - `ui_kits/{web,app}/index.html` (2) — the **Web** and **App** kit cards.

**Brand & icons**
- `assets/brand/` — Guardian wordmark (black/white SVG + JSX), G roundels, brand card.
- `assets/icons/` — Source UI icon set (SVG + JSX) and icon card.

**Components** — bundled to `window.GuardianSourceDesignSystem_8c4385` (load `_ds_bundle.js`)
- `components/buttons/` — **Button** (pill; priorities, sizes, pillar/brand themes)
- `components/forms/` — **TextInput**, **Select**, **Checkbox**, **Radio**
- `components/choice/` — **ChoiceCard**
- `components/editorial/` — **Kicker**, **Link**, **Tag** (media pill), **StarRating**
- `components/feedback/` — **InlineMessage**, **Badge**, **DotBadge**, **Spinner**
- `components/tables/` — **Table** (ruled, striped, compact, pillar-rule variants)
- plus the 46 materialised brand/icon components under `assets/`.

**UI kits & templates**
- `ui_kits/web/` — theguardian.com front page (interactive React recreation).
- `ui_kits/app/` — Guardian iOS news app (interactive feed → article, tab bar).
- `templates/web/WebFront.dc.html` — **theguardian.com — Front page** (Design Component template).
- `templates/app/AppHome.dc.html` — **Guardian iOS app — Home** (Design Component template).

**Skill & landing**
- `SKILL.md` — makes this folder usable as a Claude / Agent Skill.
- `overview.html` — a bundle-free landing page summarising the brand and linking the kits.

---

### Caveat — fonts & icons are substitutes
The Guardian's typefaces are proprietary and not redistributable, so this system uses
the nearest Google Fonts (**Source Serif 4** → Headline/Titlepiece, **Noto Serif** →
Text Egyptian, **Source Sans 3** → Text Sans). Swap the stacks in
`tokens/typography.css` for the licensed Guardian fonts in production.
