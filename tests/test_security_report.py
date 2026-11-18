import security_report as R

AUDIT = {"dependencies": [
    {"name": "requests", "version": "2.30.0", "vulns": [
        {"id": "GHSA-xxxx", "fix_versions": ["2.32.0"], "description": "Leaks auth header on redirect."}]},
    {"name": "gspread", "version": "6.2.1", "vulns": []},
]}
ALERTS = [
    {"rule": {"id": "py/x", "description": "Low thing", "security_severity_level": "low"},
     "most_recent_instance": {"location": {"path": "a.py", "start_line": 3}}, "html_url": "u1"},
    {"rule": {"id": "js/xss", "description": "DOM XSS", "security_severity_level": "high"},
     "most_recent_instance": {"location": {"path": "index.html", "start_line": 9}}, "html_url": "u2"},
]


def test_report_lists_findings_worst_first():
    report = R.build_report(AUDIT, ALERTS)
    assert "**requests 2.30.0**: GHSA-xxxx, fixed in 2.32.0" in report
    assert report.index("DOM XSS") < report.index("Low thing")
    assert report.rstrip().endswith("FINDINGS: 3")


def test_clean_report_has_zero_findings():
    report = R.build_report({"dependencies": [{"name": "a", "version": "1", "vulns": []}]}, [])
    assert "No known vulnerabilities" in report and "No open code scanning alerts" in report
    assert report.rstrip().endswith("FINDINGS: 0")


def test_api_error_is_not_a_list_of_alerts():
    # The alerts API returns an object (e.g. {"message": ...}) when scanning isn't set up yet.
    assert R.open_alerts({"message": "no analysis found"}) == []
