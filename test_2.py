# quick check: rows older than 4h and not Finished
import sqlite3
from datetime import datetime, timezone, timedelta
conn = sqlite3.connect('database/autotrader_data.db')
conn.row_factory = sqlite3.Row
now = datetime.now(timezone.utc)
rows = conn.execute("""
SELECT event_id, kickoff, inplay_status, ft_score, h_score, a_score, h_goals90, a_goals90
FROM current_matches
WHERE kickoff IS NOT NULL
  AND (inplay_status IS NULL OR inplay_status NOT IN ('Finished','Cancelled','Abandoned'))
""").fetchall()
for r in rows:
    ko = r['kickoff']
    try:
        ko_dt = datetime.fromisoformat(ko.replace('Z','+00:00'))
        if ko_dt.tzinfo is None:
            ko_dt = ko_dt.replace(tzinfo=timezone.utc)
        if now - ko_dt > timedelta(hours=4):
            print(dict(r))
    except Exception as e:
        print('parse/age error', r['event_id'], ko, e)
