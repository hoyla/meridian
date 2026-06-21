# Tabs — CSS brief

A horizontal tab bar sitting on a hairline rule, where the selected tab is marked by a
**thick brand-blue underline** that overlaps the rule, plus brand-blue label text. Optional
count **badges** flip to a solid brand pill when their tab is active.

_Adapted from the Guardian Employment Tribunals admin/review tool. Uses Guardian Source
design tokens; a resolved-values table at the end covers anyone without the token file._

## Markup

```html
<nav class="tabs">
  <a class="tab active" href="…">Auto-corrections <span class="badge">945</span></a>
  <a class="tab"        href="…">Flags <span class="badge">3</span></a>
  <a class="tab"        href="…">Award review <span class="badge">608</span></a>
  <a class="tab"        href="…">Overrides <span class="badge">250</span></a>
  <a class="tab"        href="…">Rule health</a>          <!-- badge optional -->
  <a class="tab"        href="…">Find a record</a>
  <a class="tab"        href="…">Match respondents</a>
</nav>
```

Tabs are plain `<a>` links; the **selected** one just gets an extra `active` class. (In the
source app the server adds it per current route — in another project toggle it however you
route: template conditional, JS, framework `class:active`, etc.)

## CSS (Guardian tokens)

```css
.tabs {
  display: flex;
  gap: var(--space-1);                                   /* 4px */
  background: var(--surface-primary);                    /* #ffffff */
  padding: 0 var(--space-5);                             /* 0 20px */
  border-bottom: var(--border-hairline) solid var(--border-primary);  /* 1px #dcdcdc */
}

.tab {
  padding: var(--space-3) var(--space-4);                /* 12px 16px */
  color: var(--text-secondary);                          /* #707070 */
  font-weight: var(--weight-medium);                     /* 500 */
  border-bottom: var(--border-section) solid transparent;/* 4px, invisible until active */
  margin-bottom: -1px;                                   /* pulls the 4px border down OVER the nav's 1px rule */
}

.tab:hover { text-decoration: none; color: var(--text-primary); }  /* #121212 — darken text only */

.tab.active {
  color: var(--brand-400);                               /* #052962 Guardian blue — label */
  border-bottom-color: var(--brand-400);                 /* the underline */
}

/* Count badge — neutral pill, flips to brand when its tab is active */
.badge {
  display: inline-block; min-width: 1.4em; text-align: center;
  background: var(--neutral-93);                          /* #ededed */
  color: var(--text-primary);                            /* #121212 */
  border-radius: var(--radius-pill);                     /* fully rounded */
  padding: 0 var(--space-2);                             /* 0 8px */
  font-size: var(--textsans-xsmall);                     /* 12px */
  font-weight: var(--weight-bold);                       /* 700 */
}
.tab.active .badge { background: var(--brand-400); color: var(--text-inverse); }  /* #052962 / #fff */
```

## The three details that make it work

1. **The underline is a `border-bottom` on the tab, not a separate element.** It's `4px`
   (`--border-section`, the same weight Guardian uses for its coloured section rules),
   transparent by default, coloured only on `.active`.
2. **`margin-bottom: -1px` on every tab** drops that border down so the active 4px underline
   sits *on top of* the nav's 1px bottom hairline — they align into one clean line instead of
   stacking. This is the bit people usually miss.
3. **Active state is class-driven** (`.tab.active`), so it works with any routing/JS and
   degrades to plain links with no JS.

## Resolved values (for anyone without the tokens)

| Token | Value | Used for |
|---|---|---|
| `--brand-400` | `#052962` | active label + underline + active badge bg |
| `--text-secondary` | `#707070` | resting label |
| `--text-primary` | `#121212` | hover label / badge text |
| `--text-inverse` | `#ffffff` | active badge text |
| `--surface-primary` | `#ffffff` | nav background |
| `--border-primary` | `#dcdcdc` | nav hairline rule |
| `--neutral-93` | `#ededed` | resting badge bg |
| `--border-section` | `4px` | underline thickness |
| `--space-1` / `-2` / `-3` / `-4` / `-5` | `4` / `8` / `12` / `16` / `20px` | gap / padding |
| `--border-hairline` | `1px` | nav rule thickness |
| `--radius-pill` | `62.5rem` | badge rounding (fully rounded) |
| `--weight-medium` / `-bold` | `500` / `700` | label / badge weight |
| `--textsans-xsmall` | `12px` | badge text size |
