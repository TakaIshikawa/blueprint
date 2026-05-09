"""Interactive HTML exporter with client-side features."""

from __future__ import annotations

import json
from collections import Counter
from html import escape
from pathlib import Path
from typing import Any

from blueprint.exporters.base import TargetExporter

TEMPLATES_DIR = Path(__file__).parent / "templates"

# ---------------------------------------------------------------------------
# Theme definitions
# ---------------------------------------------------------------------------

THEMES: dict[str, dict[str, str]] = {
    "default": {
        "bg": "#ffffff",
        "bg_secondary": "#f8f9fa",
        "text": "#212529",
        "text_muted": "#6c757d",
        "border": "#dee2e6",
        "primary": "#0d6efd",
        "success": "#198754",
        "warning": "#ffc107",
        "danger": "#dc3545",
        "info": "#0dcaf0",
        "card_bg": "#ffffff",
        "table_stripe": "#f2f2f2",
        "header_bg": "#343a40",
        "header_text": "#ffffff",
    },
    "dark": {
        "bg": "#1a1a2e",
        "bg_secondary": "#16213e",
        "text": "#e0e0e0",
        "text_muted": "#a0a0a0",
        "border": "#333355",
        "primary": "#4e8cff",
        "success": "#2ecc71",
        "warning": "#f39c12",
        "danger": "#e74c3c",
        "info": "#00cec9",
        "card_bg": "#16213e",
        "table_stripe": "#0f3460",
        "header_bg": "#0f3460",
        "header_text": "#e0e0e0",
    },
    "corporate": {
        "bg": "#fafafa",
        "bg_secondary": "#f0f0f0",
        "text": "#333333",
        "text_muted": "#777777",
        "border": "#cccccc",
        "primary": "#003366",
        "success": "#006633",
        "warning": "#cc6600",
        "danger": "#cc0000",
        "info": "#336699",
        "card_bg": "#ffffff",
        "table_stripe": "#e8e8e8",
        "header_bg": "#003366",
        "header_text": "#ffffff",
    },
}

STATUS_COLORS: dict[str, str] = {
    "pending": "warning",
    "in_progress": "info",
    "completed": "success",
    "blocked": "danger",
    "skipped": "text_muted",
}

STATUS_LABELS: dict[str, str] = {
    "pending": "Pending",
    "in_progress": "In Progress",
    "completed": "Completed",
    "blocked": "Blocked",
    "skipped": "Skipped",
}


# ---------------------------------------------------------------------------
# Main exporter
# ---------------------------------------------------------------------------


class HTMLExporter(TargetExporter):
    """Export execution plans as standalone interactive HTML documents."""

    def get_format(self) -> str:
        return "html"

    def get_extension(self) -> str:
        return ".html"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        self.ensure_output_dir(output_path)

        html = self.export_plan(execution_plan, implementation_brief)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        return output_path

    def export_plan(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any] | None = None,
        theme: str = "default",
    ) -> str:
        """Generate a standalone interactive HTML document for the plan."""
        theme_vars = THEMES.get(theme, THEMES["default"])
        brief = brief or {}

        tasks = plan.get("tasks") or []
        milestones = plan.get("milestones") or []
        status_counts = _count_statuses(tasks)

        sections = [
            _html_head(plan, theme_vars),
            '<body>',
            _header_section(plan, brief),
            '<main class="container">',
            _summary_section(plan, brief, status_counts),
            _metrics_section(tasks, status_counts),
            _task_table_section(tasks),
            _timeline_section(tasks, milestones),
            _dependency_section(tasks),
            '</main>',
            _javascript_section(tasks, status_counts),
            '</body>',
            '</html>',
        ]
        return "\n".join(sections)

    def export_with_charts(self, plan: dict[str, Any]) -> str:
        """Export with embedded Chart.js charts."""
        return self.export_plan(plan, theme="default")

    def export_printable(self, plan: dict[str, Any]) -> str:
        """Export a print-optimized version."""
        theme_vars = THEMES["default"]
        tasks = plan.get("tasks") or []
        status_counts = _count_statuses(tasks)

        sections = [
            _html_head(plan, theme_vars, print_mode=True),
            '<body class="print-mode">',
            _header_section(plan, {}),
            '<main class="container">',
            _summary_section(plan, {}, status_counts),
            _task_table_section(tasks),
            '</main>',
            '</body>',
            '</html>',
        ]
        return "\n".join(sections)

    def generate_standalone(
        self,
        plan: dict[str, Any],
        include_assets: bool = True,
    ) -> str:
        """Generate a fully standalone HTML document with all assets inline."""
        return self.export_plan(plan)

    def apply_custom_theme(self, html: str, theme: dict[str, str]) -> str:
        """Apply a custom color theme to generated HTML."""
        css_vars = "\n".join(
            f"  --{key.replace('_', '-')}: {value};"
            for key, value in theme.items()
        )
        custom_style = f"<style>:root {{\n{css_vars}\n}}</style>"
        return html.replace("</head>", f"{custom_style}\n</head>")


# ---------------------------------------------------------------------------
# HTML generation helpers
# ---------------------------------------------------------------------------


def _html_head(
    plan: dict[str, Any],
    theme: dict[str, str],
    *,
    print_mode: bool = False,
) -> str:
    title = escape(plan.get("id") or "Execution Plan")
    css_vars = "\n".join(
        f"  --{key.replace('_', '-')}: {value};"
        for key, value in theme.items()
    )
    print_css = """
    @media print {
      .no-print { display: none !important; }
      .task-details { display: block !important; }
      body { font-size: 10pt; }
    }
    """ if not print_mode else """
    .no-print { display: none !important; }
    .task-details { display: block !important; }
    body { font-size: 10pt; }
    """

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
:root {{
{css_vars}
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.6;
}}
.container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
.header {{
  background: var(--header-bg);
  color: var(--header-text);
  padding: 24px;
  margin-bottom: 24px;
}}
.header h1 {{ font-size: 1.5rem; margin-bottom: 4px; }}
.header .subtitle {{ opacity: 0.8; font-size: 0.9rem; }}
.card {{
  background: var(--card-bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 16px;
  margin-bottom: 20px;
}}
.card h2 {{ font-size: 1.2rem; margin-bottom: 12px; color: var(--primary); }}
.metrics-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 12px;
  margin-bottom: 20px;
}}
.metric {{
  background: var(--bg-secondary);
  border-radius: 8px;
  padding: 16px;
  text-align: center;
}}
.metric .value {{ font-size: 2rem; font-weight: bold; }}
.metric .label {{ font-size: 0.8rem; color: var(--text-muted); text-transform: uppercase; }}
.status-badge {{
  display: inline-block;
  padding: 2px 8px;
  border-radius: 12px;
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
}}
.status-pending {{ background: var(--warning); color: #000; }}
.status-in_progress {{ background: var(--info); color: #000; }}
.status-completed {{ background: var(--success); color: #fff; }}
.status-blocked {{ background: var(--danger); color: #fff; }}
.status-skipped {{ background: var(--text-muted); color: #fff; }}
.search-bar {{
  width: 100%;
  padding: 8px 12px;
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 0.9rem;
  margin-bottom: 12px;
  background: var(--bg);
  color: var(--text);
}}
.filter-bar {{
  display: flex;
  gap: 8px;
  margin-bottom: 12px;
  flex-wrap: wrap;
}}
.filter-btn {{
  padding: 4px 12px;
  border: 1px solid var(--border);
  border-radius: 16px;
  cursor: pointer;
  font-size: 0.8rem;
  background: var(--bg-secondary);
  color: var(--text);
}}
.filter-btn.active {{
  background: var(--primary);
  color: #fff;
  border-color: var(--primary);
}}
table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 0.9rem;
}}
th {{
  background: var(--bg-secondary);
  padding: 10px 12px;
  text-align: left;
  cursor: pointer;
  user-select: none;
  border-bottom: 2px solid var(--border);
  position: relative;
}}
th:hover {{ background: var(--border); }}
th .sort-icon {{ margin-left: 4px; opacity: 0.4; }}
th.sorted .sort-icon {{ opacity: 1; }}
td {{
  padding: 10px 12px;
  border-bottom: 1px solid var(--border);
  vertical-align: top;
}}
tr:nth-child(even) {{ background: var(--table-stripe); }}
tr.hidden {{ display: none; }}
.task-title {{ cursor: pointer; color: var(--primary); }}
.task-title:hover {{ text-decoration: underline; }}
.task-details {{
  display: none;
  padding: 12px;
  background: var(--bg-secondary);
  border-radius: 6px;
  margin-top: 8px;
  font-size: 0.85rem;
}}
.task-details.expanded {{ display: block; }}
.highlight {{ background: yellow; color: #000; padding: 0 2px; border-radius: 2px; }}
.timeline {{
  position: relative;
  padding: 20px 0;
}}
.timeline-item {{
  display: flex;
  align-items: flex-start;
  margin-bottom: 12px;
}}
.timeline-dot {{
  width: 12px;
  height: 12px;
  border-radius: 50%;
  margin-right: 12px;
  margin-top: 4px;
  flex-shrink: 0;
}}
.timeline-content {{ flex: 1; }}
.timeline-content .title {{ font-weight: 600; }}
.timeline-content .meta {{ font-size: 0.8rem; color: var(--text-muted); }}
.dep-graph {{
  overflow-x: auto;
  padding: 12px;
}}
.dep-node {{
  display: inline-block;
  padding: 6px 12px;
  border: 1px solid var(--border);
  border-radius: 6px;
  margin: 4px;
  font-size: 0.8rem;
  background: var(--bg-secondary);
}}
.dep-arrow {{ color: var(--text-muted); margin: 0 4px; }}
.chart-container {{
  max-width: 400px;
  margin: 0 auto;
  padding: 20px;
}}
canvas {{ max-width: 100%; }}
.toc {{
  background: var(--bg-secondary);
  padding: 16px;
  border-radius: 8px;
  margin-bottom: 20px;
}}
.toc a {{ color: var(--primary); text-decoration: none; display: block; padding: 4px 0; }}
.toc a:hover {{ text-decoration: underline; }}
{print_css}
</style>
</head>"""


def _header_section(plan: dict[str, Any], brief: dict[str, Any]) -> str:
    plan_id = escape(plan.get("id") or "")
    engine = escape(plan.get("target_engine") or "")
    repo = escape(plan.get("target_repo") or "")
    brief_title = escape(brief.get("title") or "")

    subtitle_parts = []
    if engine:
        subtitle_parts.append(f"Engine: {engine}")
    if repo:
        subtitle_parts.append(f"Repo: {repo}")
    if brief_title:
        subtitle_parts.append(brief_title)

    subtitle = " | ".join(subtitle_parts) if subtitle_parts else ""

    return f"""<header class="header">
  <h1>Execution Plan: {plan_id}</h1>
  <div class="subtitle">{escape(subtitle)}</div>
</header>"""


def _summary_section(
    plan: dict[str, Any],
    brief: dict[str, Any],
    status_counts: dict[str, int],
) -> str:
    total = sum(status_counts.values())
    completed = status_counts.get("completed", 0)
    progress = round(completed / total * 100) if total > 0 else 0
    status = escape(plan.get("status") or "draft")

    brief_summary = ""
    if brief.get("problem_statement"):
        brief_summary = f"""
  <div style="margin-top: 12px;">
    <strong>Problem:</strong> {escape(str(brief['problem_statement']))}
  </div>"""

    return f"""<nav class="toc card no-print" id="toc">
  <strong>Table of Contents</strong>
  <a href="#summary">Summary</a>
  <a href="#metrics">Metrics</a>
  <a href="#tasks">Tasks</a>
  <a href="#timeline">Timeline</a>
  <a href="#dependencies">Dependencies</a>
</nav>
<section class="card" id="summary">
  <h2>Executive Summary</h2>
  <p>Plan status: <span class="status-badge status-{status}">{escape(STATUS_LABELS.get(status, status))}</span></p>
  <p>Progress: <strong>{progress}%</strong> ({completed}/{total} tasks completed)</p>{brief_summary}
</section>"""


def _metrics_section(
    tasks: list[dict[str, Any]],
    status_counts: dict[str, int],
) -> str:
    total = sum(status_counts.values())
    total_hours = sum(t.get("estimated_hours") or 0 for t in tasks)

    complexity_counts = Counter(t.get("estimated_complexity") or "unknown" for t in tasks)
    risk_counts = Counter(t.get("risk_level") or "unknown" for t in tasks)

    metrics_html = f"""<section class="card" id="metrics">
  <h2>Metrics Dashboard</h2>
  <div class="metrics-grid">
    <div class="metric">
      <div class="value">{total}</div>
      <div class="label">Total Tasks</div>
    </div>"""

    for status_key in ["completed", "in_progress", "pending", "blocked"]:
        count = status_counts.get(status_key, 0)
        label = STATUS_LABELS.get(status_key, status_key)
        metrics_html += f"""
    <div class="metric">
      <div class="value" style="color: var(--{STATUS_COLORS.get(status_key, 'text')})">{count}</div>
      <div class="label">{escape(label)}</div>
    </div>"""

    if total_hours > 0:
        metrics_html += f"""
    <div class="metric">
      <div class="value">{total_hours:.0f}</div>
      <div class="label">Est. Hours</div>
    </div>"""

    metrics_html += """
  </div>
  <div class="chart-container no-print">
    <canvas id="statusChart"></canvas>
  </div>
</section>"""

    return metrics_html


def _task_table_section(tasks: list[dict[str, Any]]) -> str:
    rows = []
    for task in tasks:
        task_id = escape(task.get("id") or "")
        title = escape(task.get("title") or "")
        status = task.get("status") or "pending"
        status_label = escape(STATUS_LABELS.get(status, status))
        milestone = escape(task.get("milestone") or "—")
        owner = escape(task.get("owner_type") or "—")
        complexity = escape(task.get("estimated_complexity") or "—")
        hours = task.get("estimated_hours")
        hours_str = f"{hours:.1f}" if hours else "—"
        description = escape(task.get("description") or "")
        deps = ", ".join(str(d) for d in (task.get("depends_on") or []))
        criteria = task.get("acceptance_criteria") or []
        criteria_html = "".join(f"<li>{escape(str(c))}</li>" for c in criteria)

        rows.append(f"""<tr class="task-row" data-status="{escape(status)}" data-milestone="{milestone}" data-owner="{owner}">
  <td>{task_id}</td>
  <td>
    <span class="task-title" onclick="toggleDetails('{task_id}')">{title}</span>
    <div class="task-details" id="details-{task_id}">
      <p>{description}</p>
      {f'<p><strong>Dependencies:</strong> {escape(deps)}</p>' if deps else ''}
      {f'<strong>Acceptance Criteria:</strong><ul>{criteria_html}</ul>' if criteria_html else ''}
    </div>
  </td>
  <td><span class="status-badge status-{escape(status)}">{status_label}</span></td>
  <td>{milestone}</td>
  <td>{owner}</td>
  <td>{complexity}</td>
  <td>{hours_str}</td>
</tr>""")

    return f"""<section class="card" id="tasks">
  <h2>Tasks</h2>
  <input type="text" class="search-bar no-print" id="searchInput" placeholder="Search tasks..." oninput="searchTasks()">
  <div class="filter-bar no-print" id="filterBar">
    <button class="filter-btn active" onclick="filterByStatus('all')">All</button>
    <button class="filter-btn" onclick="filterByStatus('pending')">Pending</button>
    <button class="filter-btn" onclick="filterByStatus('in_progress')">In Progress</button>
    <button class="filter-btn" onclick="filterByStatus('completed')">Completed</button>
    <button class="filter-btn" onclick="filterByStatus('blocked')">Blocked</button>
  </div>
  <div style="overflow-x: auto;">
    <table id="taskTable">
      <thead>
        <tr>
          <th onclick="sortTable(0)">ID <span class="sort-icon">⇕</span></th>
          <th onclick="sortTable(1)">Title <span class="sort-icon">⇕</span></th>
          <th onclick="sortTable(2)">Status <span class="sort-icon">⇕</span></th>
          <th onclick="sortTable(3)">Milestone <span class="sort-icon">⇕</span></th>
          <th onclick="sortTable(4)">Owner <span class="sort-icon">⇕</span></th>
          <th onclick="sortTable(5)">Complexity <span class="sort-icon">⇕</span></th>
          <th onclick="sortTable(6)">Est. Hours <span class="sort-icon">⇕</span></th>
        </tr>
      </thead>
      <tbody>
        {"".join(rows)}
      </tbody>
    </table>
  </div>
</section>"""


def _timeline_section(
    tasks: list[dict[str, Any]],
    milestones: list[dict[str, Any]],
) -> str:
    items: list[str] = []

    for ms in milestones:
        name = escape(ms.get("name") or "Milestone")
        desc = escape(ms.get("description") or "")
        items.append(f"""<div class="timeline-item">
  <div class="timeline-dot" style="background: var(--primary)"></div>
  <div class="timeline-content">
    <div class="title">{name}</div>
    <div class="meta">{desc}</div>
  </div>
</div>""")

    milestone_tasks: dict[str, list[dict[str, Any]]] = {}
    no_milestone: list[dict[str, Any]] = []
    for t in tasks:
        ms = t.get("milestone")
        if ms:
            milestone_tasks.setdefault(ms, []).append(t)
        else:
            no_milestone.append(t)

    for ms_name, ms_tasks in milestone_tasks.items():
        for t in ms_tasks:
            status = t.get("status") or "pending"
            color_var = STATUS_COLORS.get(status, "text")
            title = escape(t.get("title") or "")
            task_id = escape(t.get("id") or "")
            items.append(f"""<div class="timeline-item">
  <div class="timeline-dot" style="background: var(--{color_var})"></div>
  <div class="timeline-content">
    <div class="title">{title}</div>
    <div class="meta">{task_id} | {escape(ms_name)}</div>
  </div>
</div>""")

    for t in no_milestone:
        status = t.get("status") or "pending"
        color_var = STATUS_COLORS.get(status, "text")
        title = escape(t.get("title") or "")
        task_id = escape(t.get("id") or "")
        items.append(f"""<div class="timeline-item">
  <div class="timeline-dot" style="background: var(--{color_var})"></div>
  <div class="timeline-content">
    <div class="title">{title}</div>
    <div class="meta">{task_id}</div>
  </div>
</div>""")

    return f"""<section class="card" id="timeline">
  <h2>Timeline</h2>
  <div class="timeline">
    {"".join(items)}
  </div>
</section>"""


def _dependency_section(tasks: list[dict[str, Any]]) -> str:
    dep_items: list[str] = []
    task_map = {t.get("id") or "": t for t in tasks}

    for task in tasks:
        deps = task.get("depends_on") or []
        if not deps:
            continue

        task_id = escape(task.get("id") or "")
        task_title = escape(task.get("title") or "")

        for dep_id in deps:
            dep_task = task_map.get(dep_id)
            dep_title = escape((dep_task or {}).get("title") or dep_id)
            dep_items.append(
                f'<div><span class="dep-node">{escape(str(dep_id))}: {dep_title}</span>'
                f'<span class="dep-arrow">→</span>'
                f'<span class="dep-node">{task_id}: {task_title}</span></div>'
            )

    if not dep_items:
        dep_items.append("<p>No dependencies defined.</p>")

    return f"""<section class="card" id="dependencies">
  <h2>Dependency Graph</h2>
  <div class="dep-graph">
    {"".join(dep_items)}
  </div>
</section>"""


def _javascript_section(
    tasks: list[dict[str, Any]],
    status_counts: dict[str, int],
) -> str:
    chart_data = json.dumps({
        "labels": list(STATUS_LABELS.values()),
        "data": [status_counts.get(s, 0) for s in STATUS_LABELS],
        "colors": ["#ffc107", "#0dcaf0", "#198754", "#dc3545", "#6c757d"],
    })

    return f"""<script>
// Chart.js CDN (lightweight, for status pie chart)
(function() {{
  var script = document.createElement('script');
  script.src = 'https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js';
  script.onload = function() {{
    var chartData = {chart_data};
    var ctx = document.getElementById('statusChart');
    if (ctx && typeof Chart !== 'undefined') {{
      new Chart(ctx, {{
        type: 'doughnut',
        data: {{
          labels: chartData.labels,
          datasets: [{{
            data: chartData.data,
            backgroundColor: chartData.colors,
          }}]
        }},
        options: {{
          responsive: true,
          plugins: {{
            legend: {{ position: 'bottom' }},
            tooltip: {{ enabled: true }},
          }}
        }}
      }});
    }}
  }};
  document.head.appendChild(script);
}})();

// Table sorting
var sortDirections = {{}};
function sortTable(colIndex) {{
  var table = document.getElementById('taskTable');
  var tbody = table.querySelector('tbody');
  var rows = Array.from(tbody.querySelectorAll('tr'));
  var dir = sortDirections[colIndex] === 'asc' ? 'desc' : 'asc';
  sortDirections[colIndex] = dir;

  rows.sort(function(a, b) {{
    var aVal = a.cells[colIndex].textContent.trim().toLowerCase();
    var bVal = b.cells[colIndex].textContent.trim().toLowerCase();
    var aNum = parseFloat(aVal);
    var bNum = parseFloat(bVal);
    if (!isNaN(aNum) && !isNaN(bNum)) {{
      return dir === 'asc' ? aNum - bNum : bNum - aNum;
    }}
    if (dir === 'asc') return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
    return aVal > bVal ? -1 : aVal < bVal ? 1 : 0;
  }});

  rows.forEach(function(row) {{ tbody.appendChild(row); }});

  // Update sort icons
  var ths = table.querySelectorAll('th');
  ths.forEach(function(th, i) {{
    th.classList.toggle('sorted', i === colIndex);
    var icon = th.querySelector('.sort-icon');
    if (icon) icon.textContent = i === colIndex ? (dir === 'asc' ? '↑' : '↓') : '⇕';
  }});
}}

// Status filtering
var activeFilter = 'all';
function filterByStatus(status) {{
  activeFilter = status;
  var rows = document.querySelectorAll('.task-row');
  rows.forEach(function(row) {{
    if (status === 'all' || row.dataset.status === status) {{
      row.classList.remove('hidden');
    }} else {{
      row.classList.add('hidden');
    }}
  }});

  // Update button styles
  var buttons = document.querySelectorAll('.filter-btn');
  buttons.forEach(function(btn) {{
    btn.classList.toggle('active', btn.textContent.toLowerCase().replace(' ', '_') === status || (status === 'all' && btn.textContent === 'All'));
  }});

  // Re-apply search
  searchTasks();
}}

// Search with highlighting
function searchTasks() {{
  var query = document.getElementById('searchInput').value.toLowerCase();
  var rows = document.querySelectorAll('.task-row');

  // Clear highlights
  document.querySelectorAll('.highlight').forEach(function(el) {{
    el.outerHTML = el.textContent;
  }});

  rows.forEach(function(row) {{
    if (activeFilter !== 'all' && row.dataset.status !== activeFilter) return;

    var text = row.textContent.toLowerCase();
    if (!query || text.indexOf(query) !== -1) {{
      row.classList.remove('hidden');
      if (query) {{
        highlightText(row, query);
      }}
    }} else {{
      row.classList.add('hidden');
    }}
  }});
}}

function highlightText(element, query) {{
  var walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT);
  var nodes = [];
  while (walker.nextNode()) nodes.push(walker.currentNode);

  nodes.forEach(function(node) {{
    var text = node.textContent;
    var lower = text.toLowerCase();
    var idx = lower.indexOf(query);
    if (idx === -1) return;

    var span = document.createElement('span');
    span.innerHTML = text.substring(0, idx) +
      '<span class="highlight">' + text.substring(idx, idx + query.length) + '</span>' +
      text.substring(idx + query.length);
    node.parentNode.replaceChild(span, node);
  }});
}}

// Expand/collapse task details
function toggleDetails(taskId) {{
  var details = document.getElementById('details-' + taskId);
  if (details) {{
    details.classList.toggle('expanded');
  }}
}}
</script>"""


def _count_statuses(tasks: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for task in tasks:
        status = task.get("status") or "pending"
        counts[status] = counts.get(status, 0) + 1
    return counts
