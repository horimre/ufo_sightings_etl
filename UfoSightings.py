import pandas as pd
import math
import logging
import sqlalchemy as db
from datetime import timedelta
import DbConf


class UfoSightings:

    def __init__(self, file_name, table_name):
        self._file_name = file_name
        self._table_name = table_name

    @property
    def file_name(self):
        return self._file_name

    @property
    def table_name(self):
        return self._table_name

    def _is_float(self, col) -> bool:
        try:
            float(col)
        except ValueError:
            print(self._file_name, ': float check failed. value:',  col)
            return False
        return True

    @staticmethod
    def _transform_datetime_col(df: pd.DataFrame) -> pd.DataFrame:
        logging.info("transform datetime column")
        try:

            # there are a lot of record where the time is 24:00. 24 is not a valid hour!
            # need to replace with 00, cast to date and add one day to it.

            df['datetime_replaced'] = df['datetime'].str.replace('24:00', '00:00')
            df['datetime_casted'] = pd.to_datetime(df['datetime_replaced'], format='%m/%d/%Y %H:%M')
            check_dtime = df['datetime'].str.contains('24:00')
            df['datetime'] = df['datetime_casted'] + check_dtime * timedelta(days=1)

            df.drop(columns=['datetime_replaced', 'datetime_casted'], inplace=True)

            return df

        except Exception:
            raise

    def _clean_file(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("data cleansing")

        try:

            # trim whitespaces from header (column names)
            df = df.rename(columns=lambda x: x.strip())

            # cast 'datetime' column to date
            self._transform_datetime_col(df)

            # cast 'date posted' column to date
            df["date posted"] = pd.to_datetime(df['date posted'], format='%m/%d/%Y')

            # cast 'duration (seconds)' to float
            if df["duration (seconds)"].dtype != float:
                # remove rows where it is not float
                df = df[df["duration (seconds)"].apply(self._is_float)]

                # change datatype to float
                df = df.astype({"duration (seconds)": float})

            # cast 'latitude' to float
            if df["latitude"].dtype != float:
                df = df[df["latitude"].apply(self._is_float)]
                df = df.astype({"latitude": float})

            # cast 'longitude' to float
            if df["longitude"].dtype != float:
                df = df[df["longitude"].apply(self._is_float)]
                df = df.astype({"longitude": float})

            return df

        except Exception:
            raise

    @staticmethod
    def _calc_max_length(df: pd.DataFrame, col: str) -> float:
        logging.info("calculate length for table columns")

        try:

            # check max length of column and add some buffer
            buffer = 1.2

            max_len = df[f'{col}'].str.len().max()
            col_len = math.ceil(float(max_len) * buffer)

            return col_len

        except Exception:
            raise

    def _load_to_db(self, df: pd.DataFrame):
        logging.info("create db table and load data")

        try:

            # create sql engine
            sql_engine = db.create_engine(f'mysql+pymysql://{DbConf.db_conf["user"]}:'
                                          f'{DbConf.db_conf["password"]}'
                                          f'@localhost:{DbConf.db_conf["port"]}/'
                                          f'{DbConf.db_conf["db"]}')

            # define varchar column lengths
            city_max_len = self._calc_max_length(df, 'city')
            state_max_len = self._calc_max_length(df, 'state')
            country_max_len = self._calc_max_length(df, 'country')
            shape_max_len = self._calc_max_length(df, 'shape')
            duration_max_len = self._calc_max_length(df, 'duration (hours/min)')
            comments_max_len = self._calc_max_length(df, 'comments')

            meta = db.MetaData()

            db.Table(
                self._table_name,
                meta,
                db.Column('sight_id', db.BigInteger, primary_key=True),
                db.Column('datetime', db.DateTime),
                db.Column('city', db.String(city_max_len)),
                db.Column('state', db.String(state_max_len)),
                db.Column('country', db.String(country_max_len)),
                db.Column('shape', db.String(shape_max_len)),
                db.Column('duration (seconds)', db.DECIMAL(11, 2)),
                db.Column('duration (hours/min)', db.String(duration_max_len)),
                db.Column('comments', db.String(comments_max_len)),
                db.Column('date posted', db.DateTime),
                db.Column('latitude', db.DECIMAL(11, 8)),
                db.Column('longitude', db.DECIMAL(11, 8))
            )

            meta.create_all(sql_engine)

            df.to_sql(self._table_name, sql_engine, if_exists='append', index=False)

        except Exception:
            raise

    def _extract_csv(self) -> pd.DataFrame:
        logging.info("read csv")

        try:
            df = pd.read_csv(f"{self._file_name}", on_bad_lines='warn', dtype={"datetime": str,
                                                                               "city": str,
                                                                               "state": str,
                                                                               "country": str,
                                                                               "shape": str,
                                                                               "duration (seconds)": str,
                                                                               "duration (hours/min)": str,
                                                                               "comments": str,
                                                                               "date posted": str,
                                                                               "latitude": str,
                                                                               "longitude": str})
            return df

        except Exception:
            raise

    def _transform_csv(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("transform csv (sorting, data cleansing)")
        try:
            df = df.sort_values(by='datetime')
            df_cleansed = self._clean_file(df)
            return df_cleansed
        except Exception:
            raise

    def _load_csv(self, df: pd.DataFrame):
        logging.info("load data to db")
        try:
            self._load_to_db(df)
        except Exception:
            raise

    def etl(self):
        # set log level
        logging.basicConfig(level=logging.WARNING)

        logging.info("start ETL process")

        try:
            df = self._extract_csv()
            df_cleansed = self._transform_csv(df)
            self._load_csv(df_cleansed)
        except Exception as e:
            logging.error(f"Error: {e}", exc_info=True)


if __name__ == "__main__":

    UfoSightings('scrubbed.csv', 'ufo').etl()

