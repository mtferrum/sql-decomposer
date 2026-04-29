"""Visualisation back-ends for `LineageResult`.

`render_svg(result)` returns a self-contained SVG document (no external CSS,
fonts or scripts) that lays out the lineage as:

       ┌──────────────┐                       ┌──────────────────┐
       │ source table │                       │ Output / CTE     │
       │   col rows   ├── Bezier edges ──────►│   col rows       │
       └──────────────┘                       └──────────────────┘

For every output column we render the producing expression and the set of
contributing base columns; Bezier edges connect each base column anchor
to each output column anchor, coloured by the output's `transformation_type`.

The file is dependency-free Python — it builds the SVG by string
concatenation, so `render_svg(...)` can be saved directly to `.svg` and
opened in any browser / Inkscape / Figma.

`render_dot(result)` produces a Graphviz DOT description for users who
prefer to layout via Graphviz (`dot -Tsvg in.dot -o out.svg`).
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .models import ColInfo, LineageResult, SourceColumn, TransformationType


# ---------------------------------------------------------------------------
# Visual style
# ---------------------------------------------------------------------------


_TYPE_COLORS: Dict[str, str] = {
    TransformationType.IDENTITY:        "#6b7280",
    TransformationType.EXPRESSION:      "#2563eb",
    TransformationType.AGGREGATE:       "#f59e0b",
    TransformationType.WINDOW:          "#7c3aed",
    TransformationType.LITERAL:         "#10b981",
    TransformationType.GROUPING:        "#dc2626",
    TransformationType.GENERATOR:       "#14b8a6",
    TransformationType.OUTER_REFERENCE: "#94a3b8",
}


@dataclass(frozen=True)
class _Theme:
    bg: str = "#ffffff"
    panel_bg: str = "#fafafa"
    panel_stroke: str = "#cbd5e1"
    panel_header_bg: str = "#1f2937"
    panel_header_fg: str = "#f9fafb"
    row_bg: str = "#ffffff"
    row_alt_bg: str = "#f3f4f6"
    row_text: str = "#111827"
    row_meta: str = "#6b7280"
    edge_default: str = "#94a3b8"
    title_fg: str = "#111827"
    legend_text: str = "#374151"
    font_family: str = (
        "ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, "
        "Helvetica, Arial, sans-serif"
    )
    mono_family: str = (
        "ui-monospace, SFMono-Regular, Menlo, Monaco, 'Cascadia Mono', "
        "'Liberation Mono', Consolas, monospace"
    )


_THEME = _Theme()


# Layout parameters (px)
_PAD = 28
_TITLE_H = 56
_LEGEND_H = 32
_PANEL_HEAD_H = 30
_ROW_H = 22          # source row height (single line)
_OUT_ROW_H = 36      # output row height (two lines: name+type, then expression)
_PANEL_GAP = 18
_PANEL_W_SRC = 280
_PANEL_W_OUT = 540
_COL_GAP = 220


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def render_svg(result: LineageResult) -> str:
    """Render `result` as a standalone SVG string."""
    return _SvgRenderer(result).render()


def render_dot(result: LineageResult) -> str:
    """Render `result` as a Graphviz DOT description."""
    return _DotRenderer(result).render()


# ---------------------------------------------------------------------------
# Text + HTML renderers
# ---------------------------------------------------------------------------


def render_text(result: LineageResult, *, file_name: str = "") -> str:
    """Compact human-readable summary suitable for stdout."""

    lines: List[str] = []
    if file_name:
        lines.append(f"=== {file_name} ===")
    lines.append(f"plan_kind = {result.plan_kind}")
    lines.append(f"operation = {result.operation}")
    if result.target:
        lines.append(f"target    = {result.target.fqn}")
    if result.source_tables:
        lines.append(f"sources   = {', '.join(result.source_tables)}")
    for n in result.notes:
        lines.append(f"note      = {n}")
    if result.cte_definitions:
        lines.append("ctes:")
        for name, cols in result.cte_definitions.items():
            lines.append(f"  {name}:")
            for i, c in enumerate(cols):
                lines.append(_fmt_col_text(c, i, indent=4))
    if result.columns:
        lines.append("columns:")
        for i, c in enumerate(result.columns):
            lines.append(_fmt_col_text(c, i, indent=2))
    else:
        lines.append("columns: <none>")
    return "\n".join(lines) + "\n"


def _fmt_col_text(col: ColInfo, i: int, indent: int = 2) -> str:
    pad = " " * indent
    sources = ", ".join(sorted(s.fqn for s in col.sources)) or "-"
    return (
        f"{pad}[{i:>2}] {col.name} :: {col.data_type} | "
        f"{col.transformation_type:<10} | "
        f"sources=[{sources}] | "
        f"expr={col.transformation}"
    )


def render_html(result: LineageResult, *, title: str = "Column lineage") -> str:
    """Self-contained HTML page embedding the SVG and the JSON dump."""

    import json as _json

    svg = render_svg(result)
    payload = _json.dumps(result.to_dict(), ensure_ascii=False, indent=2)
    title_text = html.escape(title)
    payload_text = html.escape(payload)
    return (
        "<!doctype html>\n"
        '<html lang="en">\n<head>\n'
        f'<meta charset="utf-8">\n<title>{title_text}</title>\n'
        "<style>\n"
        "  body{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;background:#f8fafc;color:#0f172a;}\n"
        "  header{padding:14px 22px;background:#0f172a;color:#f8fafc;}\n"
        "  header h1{margin:0;font-size:16px;font-weight:600;}\n"
        "  main{padding:16px 22px;}\n"
        "  .svg-wrap{background:white;border:1px solid #cbd5e1;border-radius:6px;padding:8px;overflow:auto;}\n"
        "  details{margin-top:18px;}\n"
        "  pre{background:#0f172a;color:#e2e8f0;padding:14px;border-radius:6px;font-size:11.5px;line-height:1.45;overflow:auto;}\n"
        "</style>\n</head>\n<body>\n"
        f"<header><h1>{title_text}</h1></header>\n"
        f'<main>\n<div class="svg-wrap">{svg}</div>\n'
        f"<details><summary>Lineage JSON</summary><pre>{payload_text}</pre></details>\n"
        "</main>\n</body>\n</html>\n"
    )


# ---------------------------------------------------------------------------
# SVG renderer
# ---------------------------------------------------------------------------


@dataclass
class _Anchor:
    x: float
    y: float


@dataclass
class _SourceColRow:
    column: str
    label: str
    anchor_right: _Anchor


@dataclass
class _SourcePanel:
    title: str
    x: float
    y: float
    w: float
    h: float
    rows: List[_SourceColRow]
    by_column: Dict[str, _SourceColRow]


@dataclass
class _OutColRow:
    info: ColInfo
    anchor_left: _Anchor
    color: str


@dataclass
class _OutPanel:
    title: str
    subtitle: str
    x: float
    y: float
    w: float
    h: float
    rows: List[_OutColRow]


class _SvgRenderer:
    def __init__(self, result: LineageResult) -> None:
        self.result = result
        self.theme = _THEME

    # ------------------------------------------------------------------ data

    def _consumers(self) -> List[Tuple[str, str, List[ColInfo]]]:
        """Return the list of `(panel_title, panel_subtitle, columns)` to
        render on the right side: the main output first, then each CTE."""
        consumers: List[Tuple[str, str, List[ColInfo]]] = []
        target_part = self.result.target.fqn if self.result.target else ""
        op = self.result.operation
        kind = self.result.plan_kind
        head_title = "Output"
        head_sub = f"{kind} :: {op}"
        if target_part:
            head_sub += f"  →  {target_part}"
        consumers.append((head_title, head_sub, self.result.columns))
        for name, cols in self.result.cte_definitions.items():
            consumers.append((f"CTE  ·  {name}", f"WITH {name}", cols))
        return consumers

    def _used_sources(self, consumers) -> Dict[str, List[str]]:
        """Group `(table_fqn -> [column_name, ...])` for every base column
        that is referenced anywhere in the lineage (output + CTE bodies).
        Order is stable: tables sorted by FQN, columns within a table
        sorted alphabetically."""
        groups: Dict[str, List[str]] = {}
        seen: set = set()
        for _, _, cols in consumers:
            for col in cols:
                for src in col.sources:
                    tbl = ".".join(p for p in (src.catalog, src.database, src.table) if p)
                    if not tbl:
                        tbl = src.table or "<unknown>"
                    key = (tbl, src.column)
                    if key in seen:
                        continue
                    seen.add(key)
                    groups.setdefault(tbl, []).append(src.column)
        return {t: sorted(c) for t, c in sorted(groups.items())}

    # ------------------------------------------------------------------ layout

    def _layout(self) -> Tuple[List[_SourcePanel], List[_OutPanel], int, int]:
        consumers = self._consumers()
        groups = self._used_sources(consumers)

        # Source panels
        src_panels: List[_SourcePanel] = []
        x_src = _PAD
        y_src = _PAD + _TITLE_H + _LEGEND_H
        cur_y = y_src
        for tbl, cols in groups.items():
            h = _PANEL_HEAD_H + max(1, len(cols)) * _ROW_H + 6
            rows: List[_SourceColRow] = []
            by_column: Dict[str, _SourceColRow] = {}
            for i, c in enumerate(cols):
                row_y_center = cur_y + _PANEL_HEAD_H + i * _ROW_H + _ROW_H / 2
                anchor = _Anchor(x_src + _PANEL_W_SRC, row_y_center)
                row = _SourceColRow(column=c, label=c, anchor_right=anchor)
                rows.append(row)
                by_column[c] = row
            panel = _SourcePanel(
                title=tbl,
                x=x_src,
                y=cur_y,
                w=_PANEL_W_SRC,
                h=h,
                rows=rows,
                by_column=by_column,
            )
            src_panels.append(panel)
            cur_y += h + _PANEL_GAP

        if not src_panels:
            cur_y = y_src + _PANEL_HEAD_H

        # Out panels
        x_out = x_src + _PANEL_W_SRC + _COL_GAP
        cur_oy = y_src
        out_panels: List[_OutPanel] = []
        for title, subtitle, cols in consumers:
            n = max(1, len(cols))
            h = _PANEL_HEAD_H + n * _OUT_ROW_H + 6
            rows: List[_OutColRow] = []
            for i, col in enumerate(cols):
                row_y_center = cur_oy + _PANEL_HEAD_H + i * _OUT_ROW_H + _OUT_ROW_H / 2
                anchor = _Anchor(x_out, row_y_center)
                color = _TYPE_COLORS.get(col.transformation_type, self.theme.edge_default)
                rows.append(_OutColRow(info=col, anchor_left=anchor, color=color))
            panel = _OutPanel(
                title=title,
                subtitle=subtitle,
                x=x_out,
                y=cur_oy,
                w=_PANEL_W_OUT,
                h=h,
                rows=rows,
            )
            out_panels.append(panel)
            cur_oy += h + _PANEL_GAP

        total_h = max(cur_y, cur_oy) + _PAD
        total_w = x_out + _PANEL_W_OUT + _PAD
        return src_panels, out_panels, total_w, total_h

    # ------------------------------------------------------------------ render

    def render(self) -> str:
        src_panels, out_panels, W, H = self._layout()

        parts: List[str] = []
        parts.append(self._svg_header(W, H))
        parts.append(self._defs())
        parts.append(self._title())
        parts.append(self._legend(W))

        # Edges first, so that boxes draw over them at the anchors.
        parts.append(self._render_edges(src_panels, out_panels))
        for sp in src_panels:
            parts.append(self._render_src_panel(sp))
        for op in out_panels:
            parts.append(self._render_out_panel(op))

        parts.append("</svg>\n")
        return "".join(parts)

    # ------------------------------------------------------------------ SVG primitives

    def _svg_header(self, w: int, h: int) -> str:
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" '
            f'xmlns:xlink="http://www.w3.org/1999/xlink" '
            f'viewBox="0 0 {w} {h}" width="{w}" height="{h}" '
            f'font-family="{self.theme.font_family}" font-size="12" '
            f'shape-rendering="geometricPrecision" '
            f'text-rendering="geometricPrecision">\n'
            f'<rect x="0" y="0" width="{w}" height="{h}" fill="{self.theme.bg}"/>\n'
        )

    def _defs(self) -> str:
        # Arrow marker per type (so each edge is clearly typed at its head).
        markers = []
        for t, color in _TYPE_COLORS.items():
            markers.append(
                f'<marker id="arr-{t}" viewBox="0 0 10 10" refX="9" refY="5" '
                f'markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
                f'<path d="M0,0 L10,5 L0,10 z" fill="{color}"/>'
                f'</marker>'
            )
        return "<defs>\n" + "\n".join(markers) + "\n</defs>\n"

    def _title(self) -> str:
        kind = self.result.plan_kind
        op = self.result.operation
        target = self.result.target.fqn if self.result.target else ""
        sources = ", ".join(self.result.source_tables) or "—"
        head = f"Column lineage  ·  {kind}  ·  {op}"
        if target:
            head += f"  →  {target}"
        sub = f"sources: {sources}    columns: {len(self.result.columns)}"
        if self.result.cte_definitions:
            sub += f"    CTEs: {len(self.result.cte_definitions)}"
        return (
            f'<g>'
            f'<text x="{_PAD}" y="{_PAD + 18}" font-size="18" font-weight="600" '
            f'fill="{self.theme.title_fg}">{html.escape(head)}</text>'
            f'<text x="{_PAD}" y="{_PAD + 38}" font-size="11" '
            f'fill="{self.theme.legend_text}">{html.escape(sub)}</text>'
            f'</g>\n'
        )

    def _legend(self, total_w: int) -> str:
        items = list(_TYPE_COLORS.items())
        chip_w = 12
        gap = 6
        item_pad = 18
        # Compute font widths conservatively (~6.6 px / char @ 11px).
        x = _PAD
        y = _PAD + _TITLE_H - 6
        parts = [
            f'<g font-size="11" fill="{self.theme.legend_text}">'
        ]
        for label, color in items:
            label_text = label
            text_w = len(label_text) * 6.8
            parts.append(
                f'<rect x="{x:.1f}" y="{y - 9:.1f}" width="{chip_w}" height="{chip_w}" '
                f'rx="2" fill="{color}"/>'
            )
            parts.append(
                f'<text x="{x + chip_w + gap:.1f}" y="{y:.1f}">{html.escape(label_text)}</text>'
            )
            x += chip_w + gap + text_w + item_pad
            if x > total_w - _PAD - 200:
                break
        parts.append("</g>\n")
        return "".join(parts)

    def _render_src_panel(self, sp: _SourcePanel) -> str:
        parts = [self._render_panel_frame(sp.x, sp.y, sp.w, sp.h, sp.title, "")]
        for i, row in enumerate(sp.rows):
            row_y = sp.y + _PANEL_HEAD_H + i * _ROW_H
            bg = self.theme.row_alt_bg if i % 2 else self.theme.row_bg
            parts.append(
                f'<rect x="{sp.x + 1}" y="{row_y}" width="{sp.w - 2}" '
                f'height="{_ROW_H}" fill="{bg}"/>'
            )
            parts.append(
                f'<text x="{sp.x + 14}" y="{row_y + _ROW_H / 2 + 4}" '
                f'fill="{self.theme.row_text}" font-family="{self.theme.mono_family}" '
                f'font-size="11.5">{html.escape(row.label)}</text>'
            )
            # Right-edge anchor dot
            ax, ay = row.anchor_right.x, row.anchor_right.y
            parts.append(
                f'<circle cx="{ax:.1f}" cy="{ay:.1f}" r="3" fill="#475569"/>'
            )
        return "".join(parts) + "\n"

    def _render_out_panel(self, op: _OutPanel) -> str:
        parts = [self._render_panel_frame(op.x, op.y, op.w, op.h, op.title, op.subtitle)]
        for i, row in enumerate(op.rows):
            row_y = op.y + _PANEL_HEAD_H + i * _OUT_ROW_H
            bg = self.theme.row_alt_bg if i % 2 else self.theme.row_bg
            parts.append(
                f'<rect x="{op.x + 1}" y="{row_y}" width="{op.w - 2}" '
                f'height="{_OUT_ROW_H}" fill="{bg}"/>'
            )
            # Anchor on left (vertically centred on the whole 2-line row)
            parts.append(
                f'<circle cx="{op.x:.1f}" cy="{row_y + _OUT_ROW_H/2:.1f}" r="3" '
                f'fill="{row.color}"/>'
            )
            # Top line: badge | name :: data_type
            top_y = row_y + 14
            badge = row.info.transformation_type
            badge_w = len(badge) * 6.6 + 12
            parts.append(
                f'<rect x="{op.x + 10}" y="{row_y + 5}" width="{badge_w:.1f}" '
                f'height="14" rx="3" fill="{row.color}"/>'
            )
            parts.append(
                f'<text x="{op.x + 16}" y="{top_y + 1.5:.1f}" '
                f'fill="white" font-size="10" font-weight="600">'
                f'{html.escape(badge)}</text>'
            )
            name = row.info.name or ""
            name_x = op.x + 18 + badge_w
            parts.append(
                f'<text x="{name_x:.1f}" y="{top_y + 1.5:.1f}" '
                f'fill="{self.theme.row_text}" font-family="{self.theme.mono_family}" '
                f'font-size="11.5" font-weight="600">{html.escape(name)}</text>'
            )
            dt = row.info.data_type or ""
            if dt:
                dt_x = name_x + len(name) * 7 + 8
                parts.append(
                    f'<text x="{dt_x:.1f}" y="{top_y + 1.5:.1f}" '
                    f'fill="{self.theme.row_meta}" font-family="{self.theme.mono_family}" '
                    f'font-size="10.5">::{html.escape(dt)}</text>'
                )
            # Bottom line: expression text, full width, monospace
            expr_full = row.info.transformation
            avail_chars = max(20, int((op.w - 36) / 6.4))
            expr = _truncate(expr_full, avail_chars)
            sources_full = ", ".join(sorted(s.fqn for s in row.info.sources)) or "—"
            tooltip = (
                f"{row.info.name} :: {row.info.data_type}\n"
                f"{row.info.transformation_type}\n"
                f"{expr_full}\n"
                f"sources: {sources_full}"
            )
            parts.append(
                f'<g><title>{html.escape(tooltip)}</title>'
                f'<text x="{op.x + 18:.1f}" y="{row_y + _OUT_ROW_H - 8:.1f}" '
                f'fill="{self.theme.row_meta}" font-family="{self.theme.mono_family}" '
                f'font-size="10.5">{html.escape(expr)}</text>'
                f'</g>'
            )
        return "".join(parts) + "\n"

    def _render_panel_frame(self, x: float, y: float, w: float, h: float, title: str, subtitle: str) -> str:
        parts = [
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" '
            f'fill="{self.theme.panel_bg}" stroke="{self.theme.panel_stroke}" '
            f'stroke-width="1"/>',
            f'<rect x="{x}" y="{y}" width="{w}" height="{_PANEL_HEAD_H}" rx="6" '
            f'fill="{self.theme.panel_header_bg}"/>',
            f'<rect x="{x}" y="{y + _PANEL_HEAD_H - 6}" width="{w}" height="6" '
            f'fill="{self.theme.panel_header_bg}"/>',
            f'<text x="{x + 12}" y="{y + 19}" fill="{self.theme.panel_header_fg}" '
            f'font-size="12" font-weight="600">{html.escape(title)}</text>',
        ]
        if subtitle:
            parts.append(
                f'<text x="{x + w - 12}" y="{y + 19}" text-anchor="end" '
                f'fill="#cbd5e1" font-size="11">{html.escape(subtitle)}</text>'
            )
        return "".join(parts)

    # ------------------------------------------------------------------ edges

    def _render_edges(self, src_panels: List[_SourcePanel], out_panels: List[_OutPanel]) -> str:
        # Build (table_fqn -> by_column) lookup
        src_by_table = {sp.title: sp.by_column for sp in src_panels}
        edges: List[str] = []
        for op in out_panels:
            for row in op.rows:
                color = row.color
                for src in row.info.sources:
                    tbl = ".".join(p for p in (src.catalog, src.database, src.table) if p) \
                        or src.table or "<unknown>"
                    panel_cols = src_by_table.get(tbl)
                    if not panel_cols:
                        continue
                    sr = panel_cols.get(src.column)
                    if not sr:
                        continue
                    p1 = sr.anchor_right
                    p2 = row.anchor_left
                    edges.append(_bezier_edge(p1, p2, color, row.info.transformation_type))
        return f'<g class="edges">\n{"".join(edges)}\n</g>\n'


def _bezier_edge(a: _Anchor, b: _Anchor, color: str, ttype: str) -> str:
    dx = max(abs(b.x - a.x) * 0.45, 60)
    cx1, cy1 = a.x + dx, a.y
    cx2, cy2 = b.x - dx, b.y
    path = f"M {a.x:.1f} {a.y:.1f} C {cx1:.1f} {cy1:.1f}, {cx2:.1f} {cy2:.1f}, {b.x:.1f} {b.y:.1f}"
    width = 1.4
    opacity = 0.85
    return (
        f'<path d="{path}" stroke="{color}" stroke-width="{width}" fill="none" '
        f'opacity="{opacity}" marker-end="url(#arr-{ttype})"/>\n'
    )


def _truncate(text: str, n: int) -> str:
    if not text:
        return ""
    text = text.replace("\n", " ").strip()
    if len(text) <= n:
        return text
    return text[: n - 1] + "…"


# ---------------------------------------------------------------------------
# DOT renderer (Graphviz)
# ---------------------------------------------------------------------------


class _DotRenderer:
    def __init__(self, result: LineageResult) -> None:
        self.result = result

    def render(self) -> str:
        r = self.result
        consumers: List[Tuple[str, str, List[ColInfo]]] = [("output", "Output", r.columns)]
        for name, cols in r.cte_definitions.items():
            consumers.append((f"cte_{name}", f"CTE: {name}", cols))

        groups: Dict[str, List[str]] = {}
        for _, _, cols in consumers:
            for col in cols:
                for src in col.sources:
                    tbl = ".".join(p for p in (src.catalog, src.database, src.table) if p) \
                        or src.table or "<unknown>"
                    if src.column not in groups.setdefault(tbl, []):
                        groups[tbl].append(src.column)

        out: List[str] = ["digraph lineage {",
                          "  rankdir=LR;",
                          "  node [shape=plaintext, fontname=\"Helvetica\"];",
                          "  edge [color=\"#94a3b8\", penwidth=1.2];"]

        # Source tables as Mrecord-like HTML labels
        for tbl, cols in groups.items():
            tid = _dot_id("src_" + tbl)
            rows = "".join(
                f'<TR><TD ALIGN="LEFT" PORT="{_dot_port(c)}">{html.escape(c)}</TD></TR>'
                for c in sorted(cols)
            )
            out.append(
                f'  {tid} [label=<'
                f'<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4" BGCOLOR="white">'
                f'<TR><TD BGCOLOR="#1f2937"><FONT COLOR="white"><B>{html.escape(tbl)}</B></FONT></TD></TR>'
                f'{rows}'
                f'</TABLE>>];'
            )

        # Output / CTE panels
        for pid, ptitle, cols in consumers:
            tid = _dot_id(pid)
            rows = []
            for col in cols:
                color = _TYPE_COLORS.get(col.transformation_type, "#6b7280")
                badge = col.transformation_type
                expr = html.escape(_truncate(col.transformation, 60))
                rows.append(
                    f'<TR>'
                    f'<TD BGCOLOR="{color}"><FONT COLOR="white"><B>{badge}</B></FONT></TD>'
                    f'<TD ALIGN="LEFT" PORT="{_dot_port(col.name)}">'
                    f'<B>{html.escape(col.name or "")}</B>'
                    f' <FONT COLOR="#6b7280">::{html.escape(col.data_type or "")}</FONT>'
                    f'</TD>'
                    f'<TD ALIGN="LEFT"><FONT FACE="monospace">{expr}</FONT></TD>'
                    f'</TR>'
                )
            body = "".join(rows)
            out.append(
                f'  {tid} [label=<'
                f'<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4" BGCOLOR="white">'
                f'<TR><TD COLSPAN="3" BGCOLOR="#1f2937"><FONT COLOR="white"><B>{html.escape(ptitle)}</B></FONT></TD></TR>'
                f'{body}'
                f'</TABLE>>];'
            )

        # Edges
        for pid, _, cols in consumers:
            tid = _dot_id(pid)
            for col in cols:
                color = _TYPE_COLORS.get(col.transformation_type, "#94a3b8")
                for src in col.sources:
                    tbl = ".".join(p for p in (src.catalog, src.database, src.table) if p) \
                        or src.table or "<unknown>"
                    src_id = _dot_id("src_" + tbl)
                    out.append(
                        f'  {src_id}:{_dot_port(src.column)}:e -> '
                        f'{tid}:{_dot_port(col.name)}:w '
                        f'[color="{color}", penwidth=1.2];'
                    )

        out.append("}")
        return "\n".join(out) + "\n"


def _dot_id(s: str) -> str:
    return '"' + s.replace('"', '\\"') + '"'


def _dot_port(s: str) -> str:
    safe = "".join(c if c.isalnum() else "_" for c in (s or "_"))
    return safe or "_"
