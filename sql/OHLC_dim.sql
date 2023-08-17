INSERT INTO OHLC_dim (code, stock_date ,open_col, high_col, low_col,close_col,average)
SELECT code, stock_date, open_col ,high_col, low_col,close_col,average_col FROM full_data;