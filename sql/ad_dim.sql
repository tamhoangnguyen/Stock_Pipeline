INSERT INTO ad_dim (code, stock_date ,adopen, adhigh, adlow,adclose,adaverage)
SELECT code, stock_date, ad_open ,ad_high, ad_low,ad_close,adaverage FROM full_data;