################################################# BIGQUERY #################################################

#SELECT AVG(score) FROM hacker_news.new

#SELECT COUNT(c) as result FROM (SELECT text, (REGEXP_EXTRACT_ALL(text, '(?i)(google)')) as c FROM hacker_news.new GROUP BY text) 

#WITH left_trim AS (
#   SELECT REGEXP_REPLACE(url, r"http://|https://", "") as result FROM hacker_news.new WHERE url != ""
#   )
# SELECT r_deletions, COUNT(*) AS count FROM (SELECT REGEXP_REPLACE(result, r"\/.+", "") AS r_deletions FROM left_trim) GROUP BY r_deletions ORDER BY count DESC

# WITH left_trim AS (
#   SELECT REGEXP_REPLACE(url, r"http://|https://|www.", "") as result FROM hacker_news.new WHERE url != ""
#   )
# SELECT r_deletions, COUNT(*) AS count FROM (SELECT REGEXP_REPLACE(result, r"\/.+", "") AS r_deletions FROM left_trim) GROUP BY r_deletions ORDER BY count DESC