"""
security_report.py
Turns pip-audit and code scanning results into the weekly security report.

Usage: python security_report.py AUDIT_JSON ALERTS_JSON > report.md

AUDIT_JSON is `pip-audit --format json` output; ALERTS_JSON is the code
scanning alerts API response (a list; anything else counts as no alerts).
The report ends with a line "FINDINGS: <n>", which the Security workflow
reads to decide whether to open, update or close the report issue.
"""

import json
import sys
from datetime import datetime, timezone


def _load(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def vulnerable_packages(audit):
    """[(name, version, [(id, fix_versions, description)])] from pip-audit JSON."""
    out = []
    for dep in (audit or {}).get("dependencies", []):
        vulns = [(v.get("id", "?"), v.get("fix_versions", []), v.get("description", ""))
                 for v in dep.get("vulns", [])]
        if vulns:
            out.append((dep.get("name", "?"), dep.get("version", "?"), vulns))
    return out


def open_alerts(alerts):
    """[(severity, rule, path:line, url)] for open code scanning alerts."""
    if not isinstance(alerts, list):
        return []
    out = []
    for a in alerts:
        rule = a.get("rule") or {}
        loc = ((a.get("most_recent_instance") or {}).get("location") or {})
        severity = rule.get("security_severity_level") or rule.get("severity") or "unknown"
        out.append((severity, rule.get("description") or rule.get("id", "?"),
                    f"{loc.get('path', '?')}:{loc.get('start_line', '?')}", a.get("html_url", "")))
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    return sorted(out, key=lambda x: order.get(str(x[0]).lower(), 9))


def build_report(audit, alerts, now=None):
    now = now or datetime.now(timezone.utc)
    packages, scans = vulnerable_packages(audit), open_alerts(alerts)
    lines = [f"Security report for {now:%Y-%m-%d} (updated weekly by the Security workflow).", ""]

    lines.append("## Python packages (pip-audit)")
    if packages:
        lines.append("")
        for name, version, vulns in packages:
            for vid, fixes, desc in vulns:
                fix = f"fixed in {', '.join(fixes)}" if fixes else "no fix released yet"
                lines.append(f"- **{name} {version}**: {vid}, {fix}. {desc[:200]}".rstrip())
        lines += ["", "Fix: raise the version in `requirements.txt` (or merge Dependabot's pull request)."]
    else:
        lines += ["", "No known vulnerabilities in the pinned packages."]

    lines += ["", "## Code scanning (CodeQL)"]
    if scans:
        lines.append("")
        lines += [f"- **{sev}**: {rule} at `{where}` {url}".rstrip() for sev, rule, where, url in scans]
    else:
        lines += ["", "No open code scanning alerts."]

    findings = sum(len(v) for _, _, v in packages) + len(scans)
    lines += ["", f"FINDINGS: {findings}"]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    print(build_report(_load(sys.argv[1], {}), _load(sys.argv[2], [])), end="")
