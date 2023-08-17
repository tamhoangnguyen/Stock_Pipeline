DROP TABLE IF EXISTS full_data;
CREATE TABLE full_data
(
    code TEXT,
    stock_date date,
    time_update TEXT,
    floor TEXT,
    typeStock TEXT,
    basicPrice DOUBLE,
    ceilingPrice DOUBLE,
    floorPrice DOUBLE,
    open_col DOUBLE,
    high_col DOUBLE,
    low_col DOUBLE,
    close_col DOUBLE,
    average_col DOUBLE,
    ad_open DOUBLE,
    ad_high DOUBLE,
    ad_low DOUBLE,
    ad_close DOUBLE,
    adaverage DOUBLE,
    nmVolumne DOUBLE,
    nmValue DOUBLE,
    ptVolumne DOUBLE,
    ptValue DOUBLE,
    changecol DOUBLE,
    adChange DOUBLE,
    pctChange DOUBLE,
    date_string TEXT,
    year_col int,
    month_col int,
    day_col int,
    date_update date,
    year_update int,
    month_update int,
    day_update int,
    hours_update int,
    minutes_update int,
    seconds_update int
            );


CREATE TABLE IF NOT EXISTS datetime_dim
            (
                date_id int PRIMARY KEY,
                stock_date date,
                stock_year int,
                stock_month int,
                stock_day int,
                date_update date,
                year_update int,
                month_update int,
                day_update int,
                time_update TEXT,
                hours_update int,
                minutes_update int,
                seconds_update int
            );

CREATE TABLE IF NOT EXISTS price_dim
            (
                price_id int PRIMARY KEY auto_increment,
                code text,
                stock_date date,
                basicPrice DOUBLE,
                ceilingPrice DOUBLE,
                floorPrice DOUBLE
            );
CREATE TABLE IF NOT EXISTS OHLC_dim
            (
                idOHLC int PRIMARY KEY auto_increment,
                code TEXT,
                stock_date date,
                open_col DOUBLE,
                high_col DOUBLE,
                low_col DOUBLE,
                close_col DOUBLE,
                average DOUBLE
            );

CREATE TABLE IF NOT EXISTS ad_dim
            (
                ad_id int PRIMARY KEY auto_increment,
                code TEXT,
                stock_date date,
                adopen DOUBLE,
                adhigh DOUBLE,
                adlow DOUBLE,
                adclose DOUBLE,
                adaverage DOUBLE
            );
CREATE TABLE IF NOT EXISTS vol_val_dim
            (
                id_vol_val_dim int PRIMARY KEY auto_increment,
                code TEXT,
                stock_date date,
                nmVolumne DOUBLE,
                nmValue DOUBLE,
                ptVolumne DOUBLE,
                ptValue DOUBLE
            );
CREATE TABLE IF NOT EXISTS change_dim
            (
                change_id int PRIMARY KEY auto_increment,
                code TEXT,
                stock_date date,
                changecol DOUBLE,
                adChange DOUBLE,
                pctChange DOUBLE
            );
CREATE TABLE IF NOT EXISTS fact_table
                (
                    code TEXT,
                    dateKey int ,
                    floor TEXT,
                    typeStock TEXT,
                    priceKey int,
                    OHLCKey int,
                    adKey int,
                    vol_val_key int,
                    changeKey int,
                    CONSTRAINT foreign_1 FOREIGN KEY (dateKey) REFERENCES datetime_dim(date_id),
                    CONSTRAINT foreign_2 FOREIGN KEY (OHLCKey) REFERENCES OHLC_dim(idOHLC),
                    CONSTRAINT foreign_3 FOREIGN KEY (priceKey) REFERENCES  price_dim(price_id),
                    CONSTRAINT foreign_4 FOREIGN KEY (vol_val_key) REFERENCES vol_val_dim(id_vol_val_dim),
                    CONSTRAINT foreign_5 FOREIGN KEY (changeKey) REFERENCES  change_dim(change_id),
                    CONSTRAINT foreign_6 FOREIGN KEY (adKey) REFERENCES  ad_dim(ad_id)
                );