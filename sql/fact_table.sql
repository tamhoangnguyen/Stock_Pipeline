INSERT INTO fact_table
SELECT full_data.code , dt.date_id,floor, typeStock, pd.price_id, 
    od.idOHLC, ad.ad_id, volval.id_vol_val_dim,cd.change_id
FROM full_data , datetime_dim as dt, OHLC_dim as od, ad_dim as ad, vol_val_dim as volval,
    price_dim as pd, change_dim as cd
WHERE ( full_data.stock_date = dt.stock_date and full_data.time_update = dt.time_update )
AND (full_data.code = pd.code AND full_data.stock_date = pd.stock_date)
AND (full_data.code = od.code AND full_data.stock_date = od.stock_date)
AND (full_data.code = ad.code AND full_data.stock_date = ad.stock_date)
AND (full_data.code = volval.code AND full_data.stock_date = volval.stock_date)
AND (full_data.code = cd.code AND full_data.stock_date = cd.stock_date);
