# Preseason checklist

Do this in January, before the first games in mid-February. The Maintenance
workflow opens this list as an issue each January.

1. **Workflows are on.** In the Actions tab, check that *Baseball Stats Scraper*,
   *Scrape Schedules* and *Maintenance* are not marked "disabled". GitHub turns
   scheduled workflows off after 60 days without repository activity; the
   Maintenance workflow re-enables them monthly, but if it was switched off too,
   enable all three by hand ("Enable workflow").
2. **Set the season.** Change `season` in `season.json` to the new spring year
   and merge it. Every schedule and stats URL is built from it; nothing else in
   the code or the sheet needs editing.
3. **Check the URLs.** Run *Preseason URL Check* (Actions tab, Run workflow).
   Every line should say OK. "NOT PUBLISHED" means the school hasn't put up the
   new season's page yet; run the check again later. A school whose site doesn't
   follow the `/schedule/<year>` or `/stats/<year>` pattern needs its URL fixed
   in `schools.json` or the Players tab.
4. **Roster.** Check the Players tab: new players added, departed players
   removed, jersey numbers current (they often change between seasons). Run
   *Check Players Sheet* as a dry run.
5. **One manual run of each scraper.** Run *Scrape Schedules* and *Baseball
   Stats Scraper*, then look at the Run_Log tab: both should say OK.
   Stats will be zero until each player appears in a game; that's expected.
6. **Dashboard.** Open the site: the header should show the new season, and
   the History tab starts empty until the new season's games are played.
   Last season's history stays in the sheet with its Season value.
