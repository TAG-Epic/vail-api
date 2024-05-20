# Vail LB scraper
> [!IMPORTANT]  
> DO NOT RUN THIS! Use https://vail-scraper.farfrom.world/metrics instead so we don't waste AEXLAB's server capacity.

## Setup notes
This needs to be run in questdb before vail-scraper starts, or you will be waiting for years waiting for your queries
```sql
CREATE table user_stats (
  user_id symbol CAPACITY 32,
  code symbol,
  value double,
  "timestamp" timestamp
) timestamp("timestamp");
ALTER TABLE user_stats ALTER COLUMN user_id ADD INDEX;
ALTER TABLE user_stats ALTER COLUMN code ADD INDEX;
```
