"""
repair_history.py
Repairs history rows written with shifted or missing columns.

A one-time column misalignment in early 2026 left some history rows with
values one column off, or without Scraped_At_UTC / Business_Date_ET / player
fields. This used to run at the start of every stats scrape, reading each
history tab in full twice; it found nothing to repair after that (checked
18 Nov 2026), so it now runs only by hand (Repair History workflow).

Dry run by default: reports what it would repair. Pass --apply to write.

Usage: python repair_history.py [--apply]
"""

import argparse

from scraper import (
    BASE_FIELDS, BATTING_COLS, DEFENSE_COLS, PITCHING_COLS, canonicalize_header,
    build_row, connect, get_effective_headers, get_headers, get_players,
    looks_like_date, looks_like_datetime, looks_like_player_id, normalize_school_name,
    row_season, to_scraped_at_utc,
)

KNOWN_DIVISIONS = {"D1", "D2", "D3", "NJCAA"}


def history_row_needs_repair(row_map):
    if not row_map:
        return False
    scraped_at = row_map.get("Scraped_At_UTC", "")
    player_id = row_map.get("PlayerID", "")
    business_date = row_map.get("Business_Date_ET", "")
    return (
        looks_like_player_id(scraped_at)
        and not looks_like_datetime(scraped_at)
        and not looks_like_player_id(player_id)
        and not looks_like_date(business_date)
    )


def history_row_missing_metadata(row_map):
    if not row_map:
        return False
    last_updated = row_map.get("Last_Updated", "")
    player_id = row_map.get("PlayerID", "")
    scraped_at = row_map.get("Scraped_At_UTC", "")
    business_date = row_map.get("Business_Date_ET", "")
    return (
        looks_like_datetime(last_updated)
        and looks_like_player_id(player_id)
        and (not looks_like_datetime(scraped_at) or not looks_like_date(business_date))
    )


def history_row_missing_player_fields(row_map):
    if not row_map:
        return False
    return (
        looks_like_datetime(row_map.get("Scraped_At_UTC", ""))
        and looks_like_date(row_map.get("Business_Date_ET", ""))
        and not looks_like_player_id(row_map.get("PlayerID", ""))
        and str(row_map.get("Name", "")).strip() in KNOWN_DIVISIONS
    )


def build_player_lookup(players, stat_type):
    lookup = {}
    for player in players:
        player_type = str(player.get("Type", "")).strip()
        matches_type = (
            (stat_type == "pitching" and player_type == "Pitcher")
            or (stat_type in ("batting", "defense") and player_type == "Hitter")
        )
        if not matches_type:
            continue
        key = (
            normalize_school_name(player.get("School", "")),
            str(player.get("Division", "")).strip(),
        )
        if key in lookup:
            lookup[key] = None
        else:
            lookup[key] = player
    return lookup


def repair_history_table(sheet, tab, target_cols, players, apply=False):
    ws = sheet.worksheet(tab)
    headers = get_headers(ws)
    effective_headers = get_effective_headers(headers, target_cols, history=True)
    print(f"    {tab} headers: {headers}")
    print(f"    {tab} mapped headers: {effective_headers}")
    legacy_headers = BASE_FIELDS + target_cols
    all_values = ws.get_all_values()
    all_records = ws.get_all_records()
    stat_type = "pitching" if tab == "Pitching_History" else "defense" if tab == "Defense_History" else "batting"
    player_lookup = build_player_lookup(players, stat_type)
    repairs = []

    for offset, raw_row in enumerate(all_values[1:]):
        row_index = offset + 2
        padded_row = raw_row + [""] * max(0, len(effective_headers) - len(raw_row))
        row_map = {effective_headers[i]: padded_row[i] for i in range(len(effective_headers))}
        record_map = {}
        if offset < len(all_records):
            record_map = {canonicalize_header(k): v for k, v in all_records[offset].items()}
        needs_shift_repair = history_row_needs_repair(row_map)
        needs_metadata_repair = history_row_missing_metadata(record_map)
        needs_player_field_repair = history_row_missing_player_fields(record_map)
        if not needs_shift_repair and not needs_metadata_repair and not needs_player_field_repair:
            continue

        if needs_shift_repair:
            legacy_values = padded_row[:len(legacy_headers)]
            source_map = {
                legacy_headers[i]: legacy_values[i] if i < len(legacy_values) else ""
                for i in range(len(legacy_headers))
            }
        elif needs_player_field_repair:
            lookup_key = (
                normalize_school_name(record_map.get("PlayerID", "")),
                str(record_map.get("Name", "")).strip(),
            )
            player = player_lookup.get(lookup_key)
            if not player:
                continue
            source_map = {
                "Last_Updated": record_map.get("Last_Updated", ""),
                "Scraped_At_UTC": record_map.get("Scraped_At_UTC", ""),
                "Business_Date_ET": record_map.get("Business_Date_ET", ""),
            }
            source_map["PlayerID"] = player.get("PlayerID", "")
            source_map["Name"] = player.get("Name", "")
            source_map["School"] = raw_row[3] if len(raw_row) > 3 else player.get("School", "")
            source_map["Division"] = raw_row[4] if len(raw_row) > 4 else player.get("Division", "")
            stat_values = raw_row[5:]
            for index, col in enumerate(target_cols):
                source_map[col] = stat_values[index] if index < len(stat_values) else ""
        else:
            source_map = record_map

        repaired_payload = {
            "Last_Updated": source_map.get("Last_Updated", ""),
            "Scraped_At_UTC": to_scraped_at_utc(source_map.get("Last_Updated", "")),
            "Business_Date_ET": str(source_map.get("Last_Updated", "")).split(" ")[0],
            "PlayerID": source_map.get("PlayerID", ""),
            "Name": source_map.get("Name", ""),
            "School": source_map.get("School", ""),
            "Division": source_map.get("Division", ""),
            "Season": source_map.get("Season", "") or str(row_season(source_map) or ""),
        }
        for col in target_cols:
            repaired_payload[col] = source_map.get(col, "")

        repaired_row = build_row(headers, repaired_payload, target_cols, history=True)
        repairs.append({
            "range": f"A{row_index}",
            "values": [repaired_row],
        })

    print(f"    {tab}: {len(repairs)} misaligned row(s)" + (" -- repaired" if apply and repairs else ""))
    for item in repairs[:20]:
        print(f"      {item['range']}: {item['values'][0][:8]}")
    if apply and repairs:
        ws.batch_update(repairs)
    return len(repairs)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Repair misaligned history rows.")
    parser.add_argument("--apply", action="store_true", help="write the repairs")
    args = parser.parse_args(argv)
    sheet = connect()
    players = get_players(sheet)
    total = 0
    for tab, cols in (("Batting_History", BATTING_COLS), ("Pitching_History", PITCHING_COLS),
                      ("Defense_History", DEFENSE_COLS)):
        total += repair_history_table(sheet, tab, cols, players, apply=args.apply)
    if total and not args.apply:
        print("\nDry run: nothing written. Run with --apply to repair.")


if __name__ == "__main__":
    main()
