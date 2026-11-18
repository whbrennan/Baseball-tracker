# Security

This is a public repository behind a public dashboard. It holds no secrets:
the Google service-account key and the spreadsheet ID live in GitHub Actions
secrets, and the sheet is read-only to the public.

## What is monitored

| What | How | Where you see it |
|---|---|---|
| Code (Python, dashboard JavaScript, workflow files) | CodeQL, on every push to main, every pull request and weekly | Security tab > Code scanning; a check on each PR |
| Python packages | pip-audit on every push, PR and weekly; Dependabot weekly | Failing "Security" check; Dependabot PRs |
| GitHub Actions versions | Dependabot weekly | Dependabot PRs |
| Leaked secrets | GitHub secret scanning and push protection (repo settings) | Security tab > Secret scanning |
| Summary | The Security workflow's weekly report | One issue labelled `security-monitor`, opened or updated when something needs fixing and closed when clear |

## Built-in protections

- Workflows run with read-only tokens unless a job needs more, and don't
  keep the token in the checkout.
- Workflow inputs reach scripts through environment variables, never pasted
  into shell commands.
- The scrapers ask Google for the Sheets scope only, not Drive.
- The dashboard escapes every value it puts into the page; schedule text
  comes from school websites and scores from ESPN, so none of it is trusted.
- Credentials files are ignored by git (`.gitignore`).

## Owner settings to keep on

In **Settings > Code security**: Dependabot alerts, Dependabot security
updates, secret scanning and push protection. CodeQL runs from the Security
workflow (advanced setup), so its default setup stays **off**: default setup
only scanned the workflow files and can't see the JavaScript in `index.html`. In **Settings > Actions >
General**: "Read repository contents permission" as the default workflow
permission.

On the Google side (#30): the sheet's link sharing is **Viewer**; the only
editors are the owner and the service account; the service account can open
this spreadsheet only; rotate the key yearly.

## If something is found

1. A leaked Google key: delete the key in Google Cloud (IAM > Service
   accounts > Keys), create a new one, and update the `GOOGLE_CREDENTIALS`
   secret. Check the sheet's version history for unexpected edits.
2. A vulnerable package: merge Dependabot's PR, or raise the pin in
   `requirements.txt`; the tests and a branch scraper run confirm nothing broke.
3. A code scanning alert: fix it in a pull request; the check shows it cleared.

## Reporting

Please report a problem privately with GitHub's "Report a vulnerability"
button on the Security tab, not in a public issue.
