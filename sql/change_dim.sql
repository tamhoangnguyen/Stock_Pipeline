INSERT INTO change_dim (code, stock_date ,changecol,adChange,pctChange)
SELECT code, stock_date, changecol,adChange,pctChange FROM full_data;