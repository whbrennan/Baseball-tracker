"""
check_espn_ids.py
Verifies every espn_team_id in schools.json against ESPN's college-baseball
team list. ESPN uses different team IDs for each sport, so an ID copied from a
football or basketball page silently matches the wrong team.

Exits non-zero if any configured ID is unknown to ESPN or belongs to a team
whose name doesn't match the school. Also warns about D1 schools with no ID.

Usage: python check_espn_ids.py
"""

import json
import sys

import requests

SCHOOLS_FILE = "schools.json"
TEAMS_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/baseball/"
    "college-baseball/teams?limit=1000"
)


def normalise(text):
    return " ".join(str(text or "").lower().replace("&", "and").split())


def name_matches(school, team):
    """True if ESPN's team plausibly is this school.

    Matches when ESPN's display name starts with the configured school name
    ("Maryland" -> "Maryland Terrapins") or the school is configured by its
    abbreviation ("ECU" -> abbreviation "ECU").
    """
    school_n = normalise(school)
    return (
        normalise(team.get("displayName")).startswith(school_n)
        or normalise(team.get("location")) == school_n
        or normalise(team.get("abbreviation")) == school_n
    )


def fetch_teams():
    resp = requests.get(TEAMS_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    teams = {}
    for sport in data.get("sports", []):
        for league in sport.get("leagues", []):
            for entry in league.get("teams", []):
                team = entry.get("team", {})
                if team.get("id"):
                    teams[str(team["id"])] = team
    return teams


def main():
    with open(SCHOOLS_FILE) as f:
        schools = json.load(f)

    teams = fetch_teams()
    if not teams:
        sys.exit("ERROR: ESPN returned no college-baseball teams.")

    problems = 0
    for cfg in schools:
        school = cfg.get("school", "?")
        espn_id = str(cfg.get("espn_team_id") or "").strip()
        if not espn_id:
            if cfg.get("division") == "D1":
                print(f"WARN  {school}: D1 school has no espn_team_id")
            continue
        team = teams.get(espn_id)
        if team is None:
            print(f"FAIL  {school}: id {espn_id} is not an ESPN college-baseball team")
            problems += 1
        elif not name_matches(school, team):
            print(f"FAIL  {school}: id {espn_id} is {team.get('displayName')}")
            problems += 1
        else:
            print(f"OK    {school}: {espn_id} = {team.get('displayName')}")

    if problems:
        sys.exit(f"\n{problems} ESPN team ID problem(s) in {SCHOOLS_FILE}.")
    print("\nAll configured ESPN team IDs match.")


if __name__ == "__main__":
    main()
