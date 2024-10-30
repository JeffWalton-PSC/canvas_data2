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

print("start")
START_ACADEMIC_YEAR = '2004'
timezone = pytz.timezone('US/Eastern')

app_path = WindowsPath(r"F:\Applications\Canvas\data2")
downloads_path = app_path / "downloads"
schemas_path = app_path / "schemas"
log_path = app_path / "logs"
   
logger.remove()
LOGGING_LEVEL = "WARNING"
logger.add(sys.stdout, level=LOGGING_LEVEL)
logger.add(sys.stderr, level=LOGGING_LEVEL)
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

# tables = ["accounts", "users", "pseudonyms", "roles", "enrollment_terms", "enrollment_states", "enrollments", "courses", "course_sections", ]
# tables = ["pseudonyms"]
tables = ["accounts", "users", "pseudonyms", "roles", "enrollment_terms", "enrollment_states", "enrollments", "course_sections", ]


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


def copy_file(source, destination, force=False):
    if force and destination.exists():
        destination.unlink()
    try:
        with destination.open(mode="xb") as file:
            file.write(source.read_bytes())
    except FileExistsError:
        logger.error(f"File {destination} exists already.")


def table_columns(table_schema: dict):
    col_names = ['key.id']
    col_dtypes = {'key.id': pd.Int64Dtype()}
    col_datetimes = []
    for column in table_schema:
        column_name = 'value.' + column
        col_names.append(column_name)
        column_def = table_schema[column]
        logger.debug(f"{column=}, {column_name=}, {column_def=}")
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
                col_dtypes[column_name] = pd.Int64Dtype()
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


async def incremental_update(base_fn, incr_fn, file_type):
    """
    """

    logger.debug(f"incremental_update({base_fn=},  {incr_fn=}, {file_type=}")
    async with DAPClient() as session:
        result = await session.get_table_schema("canvas", file_type)
        table_schema = result.schema["properties"]["value"]["properties"]
        col_names, col_dtypes, col_datetimes = table_columns(table_schema)
        logger.debug(f"{col_names=}")
        logger.debug(f"{col_dtypes=}")
        logger.debug(f"{col_datetimes=}")

    base_df = pd.read_csv(base_fn, 
                        header=0,
                        # names=col_names, 
                        dtype=col_dtypes, 
                        parse_dates=col_datetimes,
                        true_values=["Yes", "True", "TRUE"], 
                        false_values=["No", "False", "FALSE"],
                        na_values=[r'\N'],
                        )
    logger.debug(f"base_df['{file_type}']: {base_df.shape}")

    col_names.append('meta.action')
    col_dtypes['meta.action'] = 'string'
    inc_df = pd.read_csv(incr_fn, 
                        header=0,
                        # names=col_names, 
                        dtype=col_dtypes, 
                        parse_dates=col_datetimes,
                        true_values=["Yes", "True", "TRUE"], 
                        false_values=["No", "False", "FALSE"],
                        na_values=[r'\N'],
                        )
    if not inc_df.empty:
        logger.debug(f"inc_df['{file_type}']: {inc_df.shape}")
        df_out = pd.concat([
            base_df, inc_df
        ]
        )
        logger.debug(f"df_out['{file_type}']: {df_out.shape}")
        sort_cols = [c for c in df_out.columns if 'key' in c]
        # sort_cols.append('meta.ts')
        df_out = (df_out.sort_values(sort_cols + ['meta.ts'])
                        .drop_duplicates(subset=sort_cols, keep='last')
                    )
        logger.debug(f"df_out['{file_type}']: {df_out.shape}")
        df_out.to_csv(base_fn,
                        index=False,
                        )
        logger.debug(f"{base_fn} updated.")
    else:
        logger.debug(f"no update: inc_df is empty.")


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
    # only update files during the semester
    if  (now >= start_date) & (now <= end_date):
        for t in tables:
            fn = downloads_path / (f"{t}.csv")
            logger.debug(f"{t}, {fn}")
            yt_fn = downloads_path / (f"{year}{term}_{t}.csv")
            logger.debug(f"{t}, {yt_fn.exists()=}, {yt_fn}")
            if not yt_fn.exists():
                # create with Snapshot
                async with DAPClient() as session:
                    query = SnapshotQuery(format=Format.CSV, mode=None)
                    try:
                        result = await session.download_table_data(
                            "canvas", t, query, downloads_path, decompress=True
                        )
                    except Exception as err:
                        logger.error(f"{t}: {result=}")
                        logger.error(err)
                    else:
                        if result:
                            logger.info(f"copy snapshot file: {WindowsPath(result.downloaded_files[0])}, {WindowsPath(yt_fn)}")
                            copy_file(WindowsPath(result.downloaded_files[0]), WindowsPath(yt_fn))
                            if fn.exists():
                                fn.unlink()
                            logger.info(f"move snapshot file: {WindowsPath(result.downloaded_files[0])}, {WindowsPath(fn)}")
                            move_file(WindowsPath(result.downloaded_files[0]), WindowsPath(fn))
                            WindowsPath(result.downloaded_files[0]).parent.rmdir()
                        else:
                            logger.warning(f"no result from snapshot query: {t=}, {yt_fn=}")
                    # remove incremental file
                    incr_fn = downloads_path / (f"incr_{t}.csv")
                    if incr_fn.exists():
                        incr_fn.unlink()
            else:
                # Incremental update
                logger.debug(f"{yt_fn.exists()=}, {yt_fn=}")

                if last_seen_fn.exists():
                    with open(last_seen_fn, 'rb') as file:
                        last_seen = pickle.load(file)
                        logger.debug(f"{last_seen=}")
                else:
                    last_seen = start_date

                async with DAPClient() as session:
                    query = IncrementalQuery(
                        format=Format.CSV,
                        mode=None,
                        since=last_seen,
                        until=None,
                    )
                    try:
                        result = await session.download_table_data(
                            "canvas", t, query, downloads_path, decompress=True
                        )
                    except Exception as err:
                        logger.error(f"{t}: {result=}")
                        logger.error(err)
                    else:
                        if result:
                            incr_fn = downloads_path / (f"incr_{t}.csv")
                            if incr_fn.exists():
                                incr_fn.unlink()
                            logger.info(f"move incremental file: {WindowsPath(result.downloaded_files[0])}, {WindowsPath(incr_fn)}")
                            move_file(WindowsPath(result.downloaded_files[0]), WindowsPath(incr_fn))
                            WindowsPath(result.downloaded_files[0]).parent.rmdir()
                            # update yearterm_type.csv with new increment
                            await incremental_update(fn, incr_fn, t)
                            copy_file(fn, yt_fn, True)
                        else:
                            logger.warning(f"no result from incremental query: {t=}, {incr_fn=}")

        with open(last_seen_fn, 'wb') as file:
            pickle.dump(now, file )


if __name__ == "__main__":
    asyncio.run(update_cd2_data())

print("end")