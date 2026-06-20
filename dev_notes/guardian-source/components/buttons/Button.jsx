import React from 'react';

/**
 * Guardian Source button — a fully-rounded "pill". The workhorse action.
 * Priorities: primary (filled), secondary (outlined), tertiary (light outline),
 * subdued (text only). Themes recolour it: default (Guardian blue), brand
 * (yellow reader-revenue CTA), inverse (on dark), news/opinion/sport… (pillar).
 */
export function Button({
  children,
  priority = 'primary',
  size = 'default',
  theme = 'default',
  icon = null,
  iconSide = 'right',
  iconOnly = false,
  disabled = false,
  fullWidth = false,
  href,
  style = {},
  ...rest
}) {
  const sizes = {
    default: { h: 44, px: 20, fs: 'var(--textsans-large)', icon: 24 },
    small:   { h: 36, px: 16, fs: 'var(--textsans-medium)', icon: 22 },
    xsmall:  { h: 28, px: 12, fs: 'var(--textsans-small)', icon: 18 },
  };
  const themes = {
    default: { fill: 'var(--brand-400)', text: 'var(--neutral-100)', line: 'var(--brand-400)' },
    brand:   { fill: 'var(--brand-alt-400)', text: 'var(--neutral-7)', line: 'var(--brand-alt-400)' },
    inverse: { fill: 'var(--neutral-100)', text: 'var(--brand-400)', line: 'var(--neutral-100)' },
    news:    { fill: 'var(--news-400)', text: 'var(--neutral-100)', line: 'var(--news-400)' },
    opinion: { fill: 'var(--opinion-400)', text: 'var(--neutral-100)', line: 'var(--opinion-400)' },
    sport:   { fill: 'var(--sport-400)', text: 'var(--neutral-100)', line: 'var(--sport-400)' },
  };
  const s = sizes[size] || sizes.default;
  const t = themes[theme] || themes.default;

  const base = {
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 'var(--space-2)',
    height: s.h,
    minWidth: iconOnly ? s.h : 'auto',
    padding: iconOnly ? 0 : `0 ${s.px}px`,
    borderRadius: 'var(--radius-pill)',
    fontFamily: 'var(--font-sans)',
    fontWeight: 'var(--weight-bold)',
    fontSize: s.fs,
    lineHeight: 1,
    cursor: disabled ? 'not-allowed' : 'pointer',
    border: '1px solid transparent',
    width: fullWidth ? '100%' : 'auto',
    textDecoration: 'none',
    transition: 'background-color .15s ease, color .15s ease, border-color .15s ease',
    boxSizing: 'border-box',
    WebkitTapHighlightColor: 'transparent',
    opacity: disabled ? 0.45 : 1,
    ...style,
  };

  const looks = {
    primary:   { background: t.fill, color: t.text, borderColor: t.line },
    secondary: { background: 'transparent', color: 'var(--text-primary)', borderColor: 'var(--neutral-46)' },
    tertiary:  { background: 'transparent', color: 'var(--text-primary)', borderColor: 'var(--neutral-86)' },
    subdued:   { background: 'transparent', color: 'var(--brand-500)', borderColor: 'transparent', padding: iconOnly ? 0 : `0 ${s.px / 2}px` },
  };

  const Tag = href ? 'a' : 'button';
  const iconEl = icon && (
    <span style={{ display: 'inline-flex', width: s.icon, height: s.icon, alignItems: 'center', justifyContent: 'center' }} aria-hidden="true">{icon}</span>
  );

  return (
    <Tag
      href={href}
      disabled={href ? undefined : disabled}
      style={{ ...base, ...looks[priority] }}
      {...rest}
    >
      {icon && (iconSide === 'left' || iconOnly) && iconEl}
      {!iconOnly && <span>{children}</span>}
      {icon && iconSide === 'right' && !iconOnly && iconEl}
    </Tag>
  );
}

export default Button;
