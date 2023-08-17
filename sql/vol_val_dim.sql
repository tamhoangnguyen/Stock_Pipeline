INSERT INTO vol_val_dim (code, stock_date ,nmVolumne,nmValue,ptVolumne,ptValue)
SELECT code, stock_date, nmVolumne,nmValue,ptVolumne,ptValue FROM full_data;