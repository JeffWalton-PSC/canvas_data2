import asyncio
import datetime as dt
import os
import pandas as pd
import pickle
import pytz
import sys

from loguru import logger
from pathlib import WindowsPath

from dap.api import DAPClient
from dap.dap_types import Credentials, Format, IncrementalQuery, SnapshotQuery

from powercampus import select


START_ACADEMIC_YEAR = '2004'
timezone = pytz.timezone('US/Eastern')

app_path = WindowsPath(r"F:\Applications\Canvas\data2")
downloads_path = app_path / "downloads"
schemas_path = app_path / "schemas"
log_path = app_path / "logs"
data_path = app_path / "data"
   
logger.remove()
logger.add(sys.stdout, level="WARNING")
logger.add(sys.stderr, level="WARNING")
logger.add(
    log_path / "canvas_data2.log",
    rotation="monthly",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {name} | {message}",
    level="DEBUG",
)

base_url: str = os.environ["DAP_API_URL"]
logger.info(base_url)
client_id: str = os.environ["DAP_CLIENT_ID"]
logger.info(client_id)
client_secret: str = os.environ["DAP_CLIENT_SECRET"]
# logger.info(client_secret)

credentials = Credentials.create(client_id=client_id, client_secret=client_secret)
logger.info(credentials)

tables = ["accounts", "users", "pseudonyms", "roles", "enrollment_terms", "enrollment_states", "enrollments", "courses", "course_sections", ]


def current_yearterm_df() -> pd.DataFrame:
    """
    Function returns current year/term information based on today's date.

    Returns dataframe containing:
        term - string,
        year - string,
        start_of_term - datetime,
        end_of_term - datetime,
        yearterm_sort - string,
        yearterm - string
    """

    df_cal = ( select("ACADEMICCALENDAR", 
                    fields=['ACADEMIC_YEAR', 'ACADEMIC_TERM', 'ACADEMIC_SESSION', 
                            'START_DATE', 'END_DATE', 'FINAL_END_DATE'
                            ], 
                    where=f"ACADEMIC_YEAR>='{START_ACADEMIC_YEAR}' AND ACADEMIC_TERM IN ('FALL', 'SPRING', 'SUMMER')", 
                    distinct=True
                    )
              .groupby(['ACADEMIC_YEAR', 'ACADEMIC_TERM']).agg(
                  {'START_DATE': ['min'],
                   'END_DATE': ['max'],
                   'FINAL_END_DATE': ['max']
                  }
              ).reset_index()
             )
    df_cal.columns = df_cal.columns.droplevel(1)
    
    yearterm_sort = ( lambda r:
        r['ACADEMIC_YEAR'] + '01' if r['ACADEMIC_TERM']=='SPRING' else
        (r['ACADEMIC_YEAR'] + '02' if r['ACADEMIC_TERM']=='SUMMER' else
        (r['ACADEMIC_YEAR'] + '03' if r['ACADEMIC_TERM']=='FALL' else
        r['ACADEMIC_YEAR'] + '00'))
    )
    df_cal['yearterm_sort'] = df_cal.apply(yearterm_sort, axis=1)

    df_cal['yearterm'] = df_cal['ACADEMIC_YEAR'] + '.' +  df_cal['ACADEMIC_TERM'].str.title()

    df_cal = ( 
        df_cal.drop(
            columns=[
                'END_DATE'
                ]
            )
        .rename(
            columns={
                'ACADEMIC_YEAR': 'year', 
                'ACADEMIC_TERM': 'term', 
                'START_DATE': 'start_of_term', 
                'FINAL_END_DATE': 'end_of_term', 
                }
            )
        )

    return df_cal.loc[(df_cal['end_of_term'] >= dt.datetime.today())].sort_values(['end_of_term']).iloc[[0]]


def current_yearterm() -> tuple[str, str, pd.Timestamp, pd.Timestamp, str, str]:
    """
    Function returns current year/term information based on today's date.

    Returns tuple containing:
        term - string,
        year - string,
        start_of_term - datetime,
        end_of_term - datetime,
        yearterm_sort - string,
        yearterm - string
    """

    df = current_yearterm_df()
    return (df['year'].iloc[0], df['term'].iloc[0], df['start_of_term'].iloc[0], 
        df['end_of_term'].iloc[0], df['yearterm_sort'].iloc[0], df['yearterm'].iloc[0])


def move_file(source, destination):
    try:
        with destination.open(mode="xb") as file:
            file.write(source.read_bytes())
    except FileExistsError:
        logger.error(f"File {destination} exists already.")
    else:
        source.unlink()


def table_columns(table_schema: dict):
    col_names = ['key.id']
    col_dtypes = {'key.id': pd.Int64Dtype()}
    col_datetimes = []
    for column in table_schema:
        column_name = 'value.' + column
        col_names.append(column_name)
        column_def = table_schema[column]
        # logger.debug(f"{column=}, {column_name=}, {column_def=}")
        if 'type' in column_def:
            column_type = column_def['type']
        # logger.debug(f"{column=} is {column_type=}")
        if column_type == 'string':
            if 'format' in column_def:
                column_format = column_def['format']
                if column_format == 'date-time':
                    col_datetimes.append(column_name)
                    # logger.debug(f"\t{column=} 'date-time'")
                else:
                    col_dtypes[column_name] = column_def['format']
            else:
                col_dtypes[column_name] = 'string'
        elif column_type == 'integer':
            if 'format' in column_def:
                # column_format = column_def['format']
                # col_dtypes[column_name] = column_def['format']
                col_dtypes[column_name] = pd.Int64Dtype()
                # logger.debug(f"\t{column=} {col_dtypes[column]=}")
            else:
                col_dtypes[column_name] = 'integer'
        elif column_type == 'boolean':
            col_dtypes[column_name] = 'boolean'
        elif column_type == 'object':
            col_dtypes[column_name] = 'object'
        else:
            logger.warning(f"**** Add type '{column_type}' to table_columns() function for column '{column}' ({column_name}).")
        # if column in col_dtypes:
        #     logger.debug(f"\t{column=} {col_dtypes[column_name]=}")
    col_names.append('meta.ts')
    # col_dtypes['meta.ts'] = 'date-time'
    col_datetimes.append('meta.ts')

    return col_names, col_dtypes, col_datetimes


async def update_cd2_data():
    """
    """
    year, term, start_date, end_date, yt_sort, yt = current_yearterm()
    logger.debug(f"{year=}, {term=}, {yt=}")
    start_date = timezone.localize(start_date)
    end_date = timezone.localize(end_date)
    now = timezone.localize(dt.datetime.now())
    logger.debug(f"{start_date=}, {end_date=}, {now=}")
    last_seen_fn = downloads_path / 'last_seen.pkl'
    logger.debug(f"{last_seen_fn=}")
    if  (now >= start_date) & (now <= end_date):
        for t in tables:
            fn = downloads_path / (f"{year}{term}_{t}.csv")
            logger.debug(f"{t}, {fn}")
            if fn.exists():
                # Increment update
                logger.debug(f"{fn.exists()=}")
        
                async with DAPClient() as session:
                    result = await session.get_table_schema("canvas", t)
                    table_schema = result.schema["properties"]["value"]["properties"]
                    col_names, col_dtypes, col_datetimes = table_columns(table_schema)
                    logger.debug(f"{col_names=}")
                    logger.debug(f"{col_dtypes=}")
                    logger.debug(f"{col_datetimes=}")
        
                df = pd.read_csv(fn, 
                                header=0,
                                # names=col_names, 
                                dtype=col_dtypes, 
                                parse_dates=col_datetimes,
                                na_values=[r'\N']
                                )
                logger.debug(f"df['{t}']: {df.shape}")
        
                incr_fn = downloads_path / (f"{year}{term}_incr_{t}.csv")
                with open(last_seen_fn, 'rb') as file:
                    last_seen = pickle.load(file)
                async with DAPClient() as session:
                    query = IncrementalQuery(
                        format=Format.CSV,
                        mode=None,
                        since=last_seen,
                        until=None,
                    )
                    result = await session.download_table_data(
                        "canvas", t, query, downloads_path, decompress=True
                    )
                    logger.info("move_file", WindowsPath(result.downloaded_files[0]), WindowsPath(incr_fn))
                    move_file(WindowsPath(result.downloaded_files[0]), WindowsPath(incr_fn))
                    WindowsPath(result.downloaded_files[0]).parent.rmdir()
        
                col_names.append('meta.action')
                col_dtypes['meta.action'] = 'string'
        
                inc_df = pd.read_csv(incr_fn, 
                                header=0,
                                # names=col_names, 
                                dtype=col_dtypes, 
                                parse_dates=col_datetimes,
                                na_values=[r'\N']
                                )
                logger.debug(f"inc_df['{t}']: {inc_df.shape}")
        
                fn_out = downloads_path / (f"{year}{term}_{t}.csv")
                df_out = pd.concat([
                    df, inc_df
                ]
                )
                logger.debug(f"df_out['{t}']: {df_out.shape}")
                df_out = (df_out.sort_values(['key.id', 'meta.action', 'meta.ts'])
                                .drop_duplicates(subset=['key.id'],keep='last')
                         )
                logger.debug(f"df_out['{t}']: {df_out.shape}")
                df_out.to_csv(fn_out,
                              index=False,
                             )
         
            else:
                # create with Snapshot
                async with DAPClient() as session:
                    query = SnapshotQuery(format=Format.CSV, mode=None)
                    result = await session.download_table_data(
                        "canvas", t, query, downloads_path, decompress=True
                    )
                    logger.info("move_file", WindowsPath(result.downloaded_files[0]), WindowsPath(fn))
                    move_file(WindowsPath(result.downloaded_files[0]), WindowsPath(fn))
                    WindowsPath(result.downloaded_files[0]).parent.rmdir()

            with open(last_seen_fn, 'wb') as file:
                pickle.dump(now, file )


if __name__ == "__main__":
    update_cd2_data()