Fully-rounded "pill" button — the Guardian's primary call to action; use for any tappable action.

```jsx
<Button priority="primary" theme="brand">Support the Guardian</Button>
<Button priority="secondary" size="small">Read more</Button>
<Button priority="primary" icon={<ChevronRight/>} iconSide="right">Continue</Button>
```

Priorities: `primary` (filled), `secondary` (outlined), `tertiary` (light outline), `subdued` (text only).
Sizes: `default` (44px), `small` (36px), `xsmall` (28px).
Themes: `default` (Guardian blue), `brand` (signature yellow CTA), `inverse` (on dark), plus pillars `news` / `opinion` / `sport`.
Pass `iconOnly` for a circular icon button, `fullWidth` to fill the container.
