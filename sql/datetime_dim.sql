INSERT INTO datetime_dim
SELECT DISTINCT 
CAST(CONCAT(CAST(year_col as char), CAST(month_col as char),CAST(day_col as char),
CAST(hours_update as char), CAST(minutes_update as char),CAST(seconds_update as char)) as SIGNED) AS id,
stock_date, year_col,month_col,day_col,
date_update, year_update,month_update,day_update,
time_update,hours_update,minutes_update,seconds_update
FROM full_data ;