"""LLM general "one other thing of note" take — the release-level LLMSlot, v1.

A *take* (role="takes", same model as the per-finding takes), but release-level:
it reads a deterministically built SHORTLIST of notable findings that are NOT
already headlined, and the model SELECTS at most one — the single thing a desk
editor would most want flagged beyond the headline movers — written as a SHORT
PARAGRAPH (~45 words, <=60) that gives a little context then poses the leading
question. Selection, not generation; abstention (`{"pick": null}`) is first-class.

Safety (this is the most prose of all the takes, so the discipline is explicit):
- The paragraph MUST contain a question — the interrogative is the anchor (a
  question can't be lifted into copy as a fact).
- Every number must round-trip to the candidate's facts (verify_numbers).
- Context comes ONLY from the candidate's facts + our own description/labels;
  background the model can't see in the facts must be posed CONDITIONALLY.
- No named external events (rule unchanged).

PROVENANCE: a candidate's numbers can come from several findings (a +138% mover
from the YoY finding, the 27% share from a separate partner-share finding). Each
fact carries its source finding id in `prov`; the render attaches `finding/N`
per cited number. The model never emits an id (the one place it would hallucinate).
"""
from __future__ import annotations

import json
import re

from llm_framing import make_backend, verify_numbers

WORD_CAP = 60          # soft target in the prompt
WORD_HARD = 75         # reject above this

_FLIP_SHAPES = ("peak-and-fall", "failed recovery")

GENERAL_TAKE_SYSTEM_PROMPT = """You are a trade-desk research assistant for Guardian journalists.

You are given a SHORTLIST of this release's findings that are notable but are NOT
headline top-movers (those are covered separately). Pick at most ONE — the single
thing a sharp desk editor would most want flagged as "one other thing worth a look
this release" — and write it as a SHORT PARAGRAPH (aim ~45 words, max 60). You draw
NO conclusions; you point at what is worth checking.

If nothing on the shortlist rises above routine, return {"pick": null}. Abstaining
is correct on a quiet release — do NOT manufacture significance.

The paragraph opens with the finding's facts (the numbers + the context shown for
it), then poses the LEADING QUESTION that makes it worth a look. It MUST contain a
question mark. Never a flat assertion — the interrogative is the safety mechanism:
a question can't be lifted into copy as a fact.

Output JSON only:
  {"pick": {"finding_id": <id from the shortlist>, "take": "<the paragraph>"}}
  or {"pick": null}

Hard rules — violating any one silently rejects the whole output:
1. finding_id MUST be one of the shortlisted candidates. Invent nothing.
2. Every number you mention MUST appear in that candidate's facts, unchanged
   (round 34.2% -> 34%, never -> 35%).
3. Context comes ONLY from that candidate's facts and the description/labels shown
   here, NOT your own knowledge. If the significance depends on background you
   cannot see in the facts, pose it CONDITIONALLY ("if China is normally the main
   source, is this...?") — never state it as fact.
4. DO NOT name a specific external event, date, company or policy. Pointing at a
   CATEGORY is fine ("does the timing fit any trade-policy change?").
5. If the question implies its own answer, append exactly:  (NB: hypothesis, not a finding)
6. Name the scope and parties ("EU-27 imports from China"), never bare "imports".
7. <= 60 words. Prefer the genuinely surprising over the merely large.

This is a lead for a reporter to investigate, never a publishable claim."""


# --------------------------------------------------------------------------
# Shortlist (deterministic): notable, provenanced, NOT headlined
# --------------------------------------------------------------------------

def _leaf_groups(section, out):
    if section.kind == "sector_detail" and section.metrics:
        out.append(section)
    for sub in (section.sections or []):
        _leaf_groups(sub, out)


def _headline_subjects(report):
    subs = set()
    for it in (report.headline.items or []):
        g = it.subject.get("group_name") if isinstance(it.subject, dict) else None
        if g:
            subs.add(g)
    return subs


def _group_context(g):
    """A short grounded-context string from OUR data — the group's description
    (first sentence) + any editorial theme labels. NOT the model's knowledge."""
    bits = []
    intro = (getattr(g, "intro", None) or "").strip()
    if intro:
        bits.append(intro.split(". ")[0][:140])
    fac = getattr(g, "facets", None)
    themes = getattr(fac, "theme", None) if fac else None
    if themes:
        bits.append("labels: " + ", ".join(themes))
    return "; ".join(bits)


def _flip_shape(g):
    t = (g.metrics or {}).get("trajectory") or {}
    for scope in ("EU-27", "UK", "EU-27+UK"):
        for flow, shape in (t.get(scope) or {}).items():
            if isinstance(shape, str) and any(fs in shape for fs in _FLIP_SHAPES):
                return f"{scope} {flow}: {shape.split(' (')[0]}"
    return None


def build_general_shortlist(report, *, per_type=3, cap=8):
    """One entry per subject, facts + grounded context + per-fact provenance."""
    headline = _headline_subjects(report)
    groups = []
    for s in report.sections:
        _leaf_groups(s, groups)

    cand: dict[str, dict] = {}

    def add(subject, primary_fid, kind, facts, prov, *, context="", trajectory=None):
        if not subject or subject in headline or primary_fid is None:
            return
        c = cand.setdefault(subject, {
            "subject": subject, "finding_id": primary_fid, "kinds": [],
            "facts": {}, "prov": {}, "context": context, "trajectory": None,
        })
        if kind not in c["kinds"]:
            c["kinds"].append(kind)
        for k, v in facts.items():
            if v is not None:
                c["facts"][k] = v
                if prov.get(k) is not None:
                    c["prov"][k] = prov[k]
        if context and not c["context"]:
            c["context"] = context
        if trajectory and not c["trajectory"]:
            c["trajectory"] = trajectory

    # --- big sub-headline movers (top |yoy|) ---
    mover_rows = []
    for g in groups:
        pf = next((f for f in (g.findings or []) if (f.metrics or {}).get("yoy_pct") is not None), None)
        if pf:
            mover_rows.append((abs(pf.metrics.get("yoy_pct") or 0), g, pf))
    mover_rows.sort(key=lambda x: x[0], reverse=True)
    for _, g, pf in mover_rows[:per_type]:
        m = g.metrics or {}
        add(g.title, pf.finding_id, "big_mover",
            {"yoy_pct": pf.metrics.get("yoy_pct"), "flow": pf.metrics.get("flow"),
             "china_import_share": m.get("china_share_value")},
            {"yoy_pct": pf.finding_id, "china_import_share": m.get("china_share_finding")},
            context=_group_context(g), trajectory=_flip_shape(g))

    # --- high China import-dependency (level >= 0.85; no change-data available) ---
    deps = sorted(
        ((g, (g.metrics or {}).get("china_share_value")) for g in groups
         if (g.metrics or {}).get("china_share_value") is not None
         and g.metrics["china_share_value"] >= 0.85),
        key=lambda x: x[1], reverse=True,
    )
    for g, s in deps[:per_type]:
        m = g.metrics or {}
        pf = next((f for f in (g.findings or []) if (f.metrics or {}).get("yoy_pct") is not None), None)
        add(g.title, m.get("china_share_finding") or (pf.finding_id if pf else None), "china_dependency",
            {"china_import_share": s, "yoy_pct": (pf.metrics.get("yoy_pct") if pf else None)},
            {"china_import_share": m.get("china_share_finding"), "yoy_pct": (pf.finding_id if pf else None)},
            context=_group_context(g), trajectory=_flip_shape(g))

    # --- trajectory flips (peak-and-fall / failed recovery) ---
    flips = [g for g in groups if _flip_shape(g)]
    flips.sort(key=lambda g: abs((next((f for f in (g.findings or []) if (f.metrics or {}).get("yoy_pct") is not None), None) or type("x", (), {"metrics": {}})).metrics.get("yoy_pct") or 0), reverse=True)
    for g in flips[:2]:
        m = g.metrics or {}
        pf = next((f for f in (g.findings or []) if (f.metrics or {}).get("yoy_pct") is not None), None)
        tfinds = m.get("trajectory_findings") or []
        add(g.title, pf.finding_id if pf else (tfinds[0] if tfinds else None), "trajectory_flip",
            {"yoy_pct": (pf.metrics.get("yoy_pct") if pf else None),
             "china_import_share": m.get("china_share_value")},
            {"yoy_pct": (pf.finding_id if pf else None), "china_import_share": m.get("china_share_finding")},
            context=_group_context(g), trajectory=_flip_shape(g))

    # --- mirror-gap anomalies (top |zscore|, excl. bloc aggregate) ---
    mg = []
    for s in report.sections:
        if s.kind == "mirror_gap":
            for f in (s.findings or []):
                m = f.metrics or {}
                if "bloc" in str(m.get("partner", "")).lower():
                    continue
                if m.get("zscore") is not None:
                    mg.append((abs(m["zscore"]), f))
    mg.sort(key=lambda x: x[0], reverse=True)
    for _, f in mg[:2]:
        m = f.metrics or {}
        ctx = f"Rotterdam re-export hub. {m['hub_notes']}"[:140] if m.get("hub_notes") else ""
        add(f"China-{m.get('partner')} mirror gap", f.finding_id, "mirror_gap",
            {"gap_pct": m.get("gap_pct"), "excess_pct": m.get("excess_pct"), "zscore": m.get("zscore")},
            {"gap_pct": f.finding_id, "excess_pct": f.finding_id, "zscore": f.finding_id},
            context=ctx)

    out = list(cand.values())

    def score(c):
        f = c["facts"]
        return (len(c["kinds"]),
                abs(f.get("yoy_pct") or 0) + (f.get("china_import_share") or 0) + abs(f.get("zscore") or 0) / 2)

    out.sort(key=score, reverse=True)
    return out[:cap]


# --------------------------------------------------------------------------
# Prompt assembly
# --------------------------------------------------------------------------

def _fmt_numbers(f):
    bits = []
    if f.get("yoy_pct") is not None:
        bits.append(f"{(f.get('flow') or '')} YoY {f['yoy_pct']:+.0%}".strip())
    if f.get("china_import_share") is not None:
        bits.append(f"China import share {f['china_import_share']:.0%}")
    if f.get("gap_pct") is not None:
        bits.append(f"mirror gap {f['gap_pct']:+.0%} (excess vs baseline {f.get('excess_pct', 0):+.0%}, z {f.get('zscore', 0):+.2f})")
    return "; ".join(bits)


def assemble_general_prompt(shortlist):
    lines = []
    for c in shortlist:
        lines.append(f"- finding_id {c['finding_id']} | {c['subject']} [{', '.join(c['kinds'])}]")
        lines.append(f"    numbers: {_fmt_numbers(c['facts'])}")
        if c.get("trajectory"):
            lines.append(f"    trajectory: {c['trajectory']}")
        if c.get("context"):
            lines.append(f"    context: {c['context']}")
    user = (
        "SHORTLIST — notable findings this release that are NOT headline movers.\n"
        "Pick at most one (or abstain). Cite only the numbers shown; use only the\n"
        "context shown (or pose missing background conditionally).\n\n"
        + "\n".join(lines)
        + '\n\nOutput JSON only: {"pick": {"finding_id": <id>, "take": "<= 60-word paragraph"}} or {"pick": null}'
    )
    return GENERAL_TAKE_SYSTEM_PROMPT, user


# --------------------------------------------------------------------------
# Generate + validate
# --------------------------------------------------------------------------

def _parse_pick(raw):
    s = raw.strip()
    if "{" not in s:
        return None
    try:
        return json.loads(s[s.find("{"):s.rfind("}") + 1])
    except Exception:
        return None


def _validate(pick, shortlist):
    """Return (ok, candidate, reason). Enforces: finding_id on the shortlist,
    a question present, word cap, and every number round-trips to facts."""
    by_id = {c["finding_id"]: c for c in shortlist}
    cand = by_id.get(pick.get("finding_id"))
    take = (pick.get("take") or "").strip()
    if cand is None:
        return False, None, "finding_id not on shortlist"
    if "?" not in take:
        return False, cand, "no question (interrogative anchor missing)"
    if len(take.split()) > WORD_HARD:
        return False, cand, f"too long ({len(take.split())} words)"
    num_ok, fails = verify_numbers(take, cand["facts"])
    if not num_ok:
        return False, cand, f"unverified number(s): {fails}"
    return True, cand, ""


def _citations(take, cand):
    """The distinct source findings backing the numbers actually cited."""
    fids = []
    for k, fid in (cand.get("prov") or {}).items():
        val = cand["facts"].get(k)
        if isinstance(val, float) and f"{abs(val) * 100:.0f}" in re.sub(r"[^0-9]", " ", take):
            if fid not in fids:
                fids.append(fid)
    if not fids and cand.get("finding_id"):
        fids = [cand["finding_id"]]
    return fids


def generate_general_take(report, backend=None):
    """Build shortlist -> select -> validate. Returns
    {finding_id, take, citations, subject} or None (abstain / no candidates /
    rejected)."""
    shortlist = build_general_shortlist(report)
    if not shortlist:
        return None
    system, user = assemble_general_prompt(shortlist)
    backend = backend or make_backend(role="takes")
    pick = _parse_pick(backend.generate(system, user))
    if not pick or pick.get("pick") in (None, "null"):
        return None
    pick = pick["pick"]
    ok, cand, reason = _validate(pick, shortlist)
    if not ok:
        return None
    return {
        "finding_id": cand["finding_id"],
        "subject": cand["subject"],
        "take": pick["take"].strip(),
        "citations": _citations(pick["take"], cand),
    }


if __name__ == "__main__":
    from report_builder import build_report

    r = build_report(source_trigger="eurostat")
    sl = build_general_shortlist(r)
    print(f"=== SHORTLIST ({len(sl)}; headline excluded: {sorted(_headline_subjects(r))}) ===")
    for c in sl:
        print(f"  [{c['finding_id']}] {c['subject']}  {c['kinds']}")
        print(f"      numbers: {_fmt_numbers(c['facts'])}")
        if c.get("trajectory"):
            print(f"      trajectory: {c['trajectory']}")
        if c.get("context"):
            print(f"      context: {c['context']}")
        print(f"      cite: {c['prov']}")
    system, user = assemble_general_prompt(sl)
    try:
        raw = make_backend(role="takes").generate(system, user)   # single call
        print("\n=== MODEL RAW ===\n" + raw)
        obj = _parse_pick(raw)
        pick = (obj or {}).get("pick")
        if not pick:
            print("\n=== RESULT: abstained (pick: null) ===")
        else:
            ok, cand, reason = _validate(pick, sl)
            print(f"\n=== VALIDATION ===\n  ok={ok}  reason={reason or 'all guards passed'}")
            if ok:
                cites = _citations(pick["take"], cand)
                print(f"  take ({len(pick['take'].split())} words): {pick['take']}")
                print(f"  citations: {[f'finding/{x}' for x in cites]}")
    except Exception as e:
        print(f"\n[model call skipped/failed: {type(e).__name__}: {str(e)[:200]}]")
