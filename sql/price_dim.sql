INSERT INTO price_dim (code, stock_date ,basicPrice, ceilingPrice, floorPrice)
SELECT code, stock_date, basicPrice , ceilingPrice, floorPrice FROM full_data;