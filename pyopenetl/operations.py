import csv
import datetime
import gc
import io
import json
import logging
import requests
import tarfile
from typing import Generator, Union
import time
import pandas as pd
import sqlalchemy

from pyopenetl.connections import (
    HerokuConnection,
    CloudSQLConnection,
    BQConnection,
    PostgresConnection,
)


class BaseReader:
    """
    Basic class inherited by other Reader classes.

    args:
        source_conn: a connection object which will be used to open up a
        context-managed connection to one of our databases.
    """

    def __init__(
        self,
        source_conn: Union[
            HerokuConnection, CloudSQLConnection, BQConnection, PostgresConnection
        ],
    ) -> None:
        self.source_conn = source_conn

    def table_to_dataframe(
        self, table: str, chunksize: int = 100000
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Yields a generator that reads chunks from a SQL table into a dataframe.

        args:
            table: the name of the table to read into a dataframe
            chunksize: the number of rows to read in at a time
        """
        query = f"SELECT * FROM {table}"
        for df in self.sql_to_dataframe(query, chunksize):
            yield df

    def sql_to_dataframe(
        self, query: str, chunksize: int = 100000
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Yields a generator that reads chunks from a SQL table into a dataframe with a
        custom query. Really just a wrapper around pd.read_sql that easily gives us a
        generator.

        args:
            query: the text of your desired sql query
            chunksize: the number of rows to read in at a time
        """
        with self.source_conn.connect() as conn:
            for df in pd.read_sql(query, con=conn, chunksize=chunksize):
                yield df


class HerokuReader(BaseReader):
    """
    A Heroku Postgres-specific Reader object which inherits from the BaseReader class.
    """

    def __init__(self, source_conn: HerokuConnection) -> None:
        if not isinstance(source_conn, HerokuConnection):
            raise TypeError(
                f"HerokuReader requires instantiation with a HerokuConnection object,\
                tried with {source_conn}"
            )
        super().__init__(source_conn)


class CloudSQLReader(BaseReader):
    """
    A Cloud SQL-specific Reader object which inherits from the BaseReader class.
    """

    def __init__(self, source_conn: CloudSQLConnection) -> None:
        if not isinstance(source_conn, CloudSQLConnection):
            raise TypeError(
                f"CloudSQLReader requires instantiation with a CloudSQLConnection object\
                    tried with {source_conn}"
            )
        super().__init__(source_conn)


class BaseWriter:
    """
    Takes a source connection and a destination connection object and provides methods
    for writing data between the two.

    args:
        source_conn: the source connection object we'll be using
        dest_conn: the destination connection object we'll be using
    """

    def __init__(
        self,
        source_conn: Union[
            HerokuConnection, CloudSQLConnection, BQConnection, PostgresConnection, None
        ],
        dest_conn: Union[
            HerokuConnection, CloudSQLConnection, BQConnection, PostgresConnection
        ],
    ) -> None:
        self.source_conn = source_conn
        self.dest_conn = dest_conn

    def convert_column_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sets up types properly for ID and timestamp columns."""
        for col in df.columns:
            if "_at" in col:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        df = df.convert_dtypes()

        return df

    def write_from_dataframe(
        self,
        table: str,
        df: pd.DataFrame,
        chunksize: int = 100_000_000,
        table_schema: Union[None, str] = None,
        dtypes: dict = {},
    ) -> str:
        """
        Writes a pandas dataframe to a given SQL table. Note that this assumes that the
        table we're going to write to already exists -- it will NOT create the table for
        you.

        args:
            table: the name of the pre-existing table to write out to
            df: the data to write
            chunksize: the number of rows to write out at once. default to large enough
                to write the whole df at once.
            table_schema: the SQL schema of the table to write out to
            dtypes: a dictionary of column names to SQLAlchemy data types
        """

        # we need to define this here instead of as a class method,
        # bc pandas expects 4 args but as a class method it'll have 5
        def buffer_write(table, conn, keys, data_iter):
            """
            Execute SQL statement inserting data (borrowed from pandas)

            args:
                table: pandas.io.sql.SQLTable
                conn: sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
                keys: list of str column names
                data_iter: Iterable that iterates the values to be inserted
            """
            # gets a DBAPI connection that can provide a cursor
            raw_conn = conn.connection
            with raw_conn.cursor() as cur:
                s_buf = io.StringIO()
                writer = csv.writer(s_buf)
                writer.writerows(data_iter)
                s_buf.seek(0)

                if table.schema:
                    table_name = "{}.{}".format(table.schema, table.name)
                else:
                    table_name = table.name

                sql = f"COPY {table_name} FROM STDIN WITH CSV"
                cur.copy_expert(sql=sql, file=s_buf)

        with self.dest_conn.connect() as sql_conn:
            with sql_conn.begin():
                df = self.convert_column_types(df)
                n_rows = int(df.shape[0])
                try:
                    if len(dtypes) > 0:
                        df.to_sql(
                            name=table,
                            schema=table_schema,
                            con=sql_conn,
                            if_exists="append",
                            index=False,
                            chunksize=chunksize,
                            method=buffer_write,
                            dtype=dtypes,
                        )
                    else:
                        df.to_sql(
                            name=table,
                            schema=table_schema,
                            con=sql_conn,
                            if_exists="append",
                            index=False,
                            chunksize=chunksize,
                            method=buffer_write,
                        )
                except Exception as err:
                    raise err from err

                del df
                gc.collect()

        return f"Wrote {n_rows} rows to Cloud SQL table {table}"

    def execute_raw_sql(self, sql: str, use_textual: bool = False):
        """
        Executes raw SQL on the given destination connection. This method
        makes no attempt to validate the SQL being sent to the destination
        connection, so do not use this method in a context involving user
        input.

        """

        if use_textual:
            sql = sqlalchemy.text(sql)

        start = datetime.datetime.now()
        with self.dest_conn.connect() as sql_conn:
            res = sql_conn.execute(sql)

        print(f"SQL executed in {datetime.datetime.now() - start}")
        return res

    def get_postgres_table_schema(self, table: str) -> dict:
        """
        Turns a SQL table's schema into a dictionary of column names to SQLAlchemy
        types.

        args:
            table: name of the table schema to read
        """

        def _string_types_to_sqlachemy_types(str_type):
            type_mapping = {
                "ARRAY": sqlalchemy.dialects.postgresql.ARRAY,
                "bigint": sqlalchemy.dialects.postgresql.BIGINT,
                "boolean": sqlalchemy.dialects.postgresql.BOOLEAN,
                "character varying": sqlalchemy.dialects.postgresql.VARCHAR,
                "date": sqlalchemy.dialects.postgresql.DATE,
                "double precision": sqlalchemy.dialects.postgresql.DOUBLE_PRECISION,
                "inet": sqlalchemy.dialects.postgresql.INET,
                "integer": sqlalchemy.dialects.postgresql.INTEGER,
                "jsonb": sqlalchemy.dialects.postgresql.JSONB,
                "bytea": sqlalchemy.dialects.postgresql.BYTEA,
                "numeric": sqlalchemy.dialects.postgresql.NUMERIC,
                "smallint": sqlalchemy.dialects.postgresql.SMALLINT,
                "timestamp with time zone": sqlalchemy.dialects.postgresql.TSTZRANGE,
                "text": sqlalchemy.dialects.postgresql.TEXT,
                "timestamp without time zone": sqlalchemy.dialects.postgresql.TIMESTAMP,
            }
            try:
                type_to_return = type_mapping[str_type]
            except Exception as err:
                raise ValueError(
                    f"Unable to map type: {str_type}. Does it exist in type_mappings?\
                        {err}"
                )

            return type_to_return

        with self.dest_conn.connect() as conn:
            res = conn.execute(
                f"""
                SELECT
                    column_name,
                    data_type
                FROM
                    information_schema.columns
                WHERE
                    table_name = '{table}';
                """
            )
            cols_to_types = res.all()
            dtypes = {
                i[0]: _string_types_to_sqlachemy_types(i[1]) for i in cols_to_types
            }

        return dtypes


class CloudSQLWriter(BaseWriter):
    """
    A Cloud SQL-specific writer classes which has its own methods for CRUD operations
    on Cloud SQL tables.

    args:
        source_conn: The connection object for the source we want to read from
        dest_conn: Must be a CloudSQLConnection object
    """

    def __init__(
        self,
        source_conn: Union[
            HerokuConnection, CloudSQLConnection, BQConnection, PostgresConnection
        ],
        dest_conn: CloudSQLConnection,
    ) -> None:
        assert isinstance(dest_conn, CloudSQLConnection), TypeError(
            "Connection type must be a CloudSQLConnection"
        )
        super().__init__(source_conn, dest_conn)

    def create_table_from_dataframe(
        self, table: str, df: pd.DataFrame, dtypes: dict = {}, primary_key: str = "id"
    ) -> None:
        """
        Creates an empty table with the correct column names and types.

        args:
            table: name of the table to create in Cloud SQL
            df: the dataframe we're using to set column names and types
            dtypes: a dictionary of column names to sqlalchemy data types
            primary_key: the primary key of the table we're creating
        """
        with self.dest_conn.connect() as pd_conn:
            df = self.convert_column_types(df)

            if len(dtypes) > 0:
                df.head(0).to_sql(
                    name=table,
                    con=pd_conn,
                    if_exists="replace",
                    index=False,
                    dtype=dtypes,
                )
            else:
                df.head(0).to_sql(
                    name=table,
                    con=pd_conn,
                    if_exists="replace",
                    index=False,
                )

        with self.dest_conn.connect() as cloud_sql_conn:
            # in order to do upserts later, each table needs to have a unique
            # constraint on the primary key
            if primary_key in df.columns:
                cloud_sql_conn.execute(
                    f"""
                    ALTER TABLE {table}
                    ADD CONSTRAINT {table}_{primary_key}_unique UNIQUE ({primary_key});
                    """
                )

        del df
        gc.collect()

    def delete_table(self, table: str) -> None:
        """
        Drops a table from cloud SQL if it exists.

        args:
            table: name of the table to drop
        """
        with self.dest_conn.connect() as cloud_sql_conn:
            print(f"--> Deleting table {table}")
            cloud_sql_conn.execute(f"DROP TABLE IF EXISTS {table}")

    def get_indices_from_heroku(
        self, read_table: str, write_table: str, schema: str = "public"
    ) -> str:
        """
        Reads the index commands from the source heroku table, and returns a command to
        run on the destination table.

        args:
            read_table: the name of the heroku table being read from
            write_table: the name of the Cloud SQL table we write to
        """

        # Query to get indices from read_table
        get_table_indices_query = f"""SELECT
                                tablename,
                                indexname,
                                indexdef
                            FROM
                                pg_indexes
                            WHERE
                                schemaname = '{schema}' AND
                                tablename = '{read_table}'
                            ORDER BY
                                tablename,
                                indexname;"""
        with self.source_conn.connect() as conn:
            index_df = pd.read_sql(get_table_indices_query, con=conn)

        final_query_str = ""
        # Clean the index queries and concatenate them into one nice (long) query
        for query in index_df["indexdef"]:
            tempstr = query.replace(f"ON {schema}.{read_table}", f"ON {write_table}")
            final_query_str += tempstr
            final_query_str += "; \n"
        return final_query_str

    def seed_table(
        self,
        read_table: str,
        read_chunksize: int,
        write_table: str,
        schema: str = "public",
    ) -> str:
        """
        Seeds a direct projection of a table from one DB source to another. Note that
        this DOES create the table before writing, unlike the basic write method.

        args:
            read_table: the name of the table to read from
            read_chunksize: the number of rows to read from read_table at a time
            write_table: the name of the table to write out to cloud SQL. This parameter
                allows seed_table() to seed one table (e.g., users) into Cloud SQL under
                a different name (e.g., users_projection).
        """
        # delete the table we want to write out in Cloud SQL
        self.delete_table(write_table)

        if isinstance(self.source_conn, HerokuConnection):
            reader = HerokuReader(self.source_conn)
        elif isinstance(self.source_conn, CloudSQLConnection):
            reader = CloudSQLReader(self.source_conn)
        else:
            raise TypeError(
                f"Unsupported source connection type {self.source_conn} for table\
                    seeding."
            )

        write_time = time.time()

        # read from postgres
        its = 1
        for df in reader.table_to_dataframe(table=read_table, chunksize=read_chunksize):
            print(f"Batch: {(its-1)*read_chunksize} to {(its)*read_chunksize}")
            if its == 1:
                # write to (an optionally different) table name in Cloud SQL
                print(f"--> Creating new table {write_table} in Cloud SQL")
                self.create_table_from_dataframe(
                    write_table, df, dtypes=self.get_postgres_table_schema(read_table)
                )

            try:
                self.write_from_dataframe(
                    table=write_table, df=df, chunksize=read_chunksize
                )
            except Exception as err:
                return f"Failed to write chunk of dataframe to {write_table}: {err}"

            # explicitly mark df for garbage collection
            del df
            gc.collect()

            its += 1

        try:
            logging.info(
                self.add_indices_to_table(
                    read_table=read_table, write_table=write_table
                )
            )
        except Exception as err:
            logging.critical(f"Failed to write indices to {write_table}: {err}")
            raise RuntimeError(
                f"Failed to add indices to CloudSQL table {read_table}: {err}"
            )

        return f"Seeding of Cloud SQL table {write_table} complete in\
            {time.time() - write_time}"

    def add_indices_to_table(
        self, read_table: str, write_table: str, schema: str = "public"
    ):
        index_creation_query = self.get_indices_from_heroku(
            read_table=read_table, write_table=write_table, schema=schema
        )
        with self.dest_conn.connect() as cloud_sql_conn:
            cloud_sql_conn.execute(index_creation_query)
        return f"Added indices to CloudSQL table {read_table}"

    def seed_from_remote_csv(
        self, remote_csv_url: str, write_table: str, chunksize: int = 200_000
    ) -> str:
        """
        Reads chunks of a dataframe from a remote filepath
        """
        its = 1
        nrows = 0
        start = datetime.datetime.now()
        for df in pd.read_csv(remote_csv_url, chunksize=200_000):
            print(f"Seeding batch {its}")
            if its == 1:
                # write to (an optionally different) table name in Cloud SQL
                print(f"--> Creating new table {write_table} in Cloud SQL")
                self.create_table_from_dataframe(write_table, df)

            try:
                self.write_from_dataframe(table=write_table, df=df, chunksize=chunksize)
                nrows += df.shape[0]
            except Exception as err:
                return f"Failed to write chunk of dataframe to {write_table}: {err}"

            del df
            gc.collect()

            its += 1

        return f"Seeded {nrows} rows in {datetime.datetime.now() - start}"

    def update_table_via_upsert(
        self,
        read_table: str,
        data_interval_hours: int,
        write_table: str,
        write_table_primary_key: str,
    ) -> str:
        """
        Performs an upsert of a small timeframe of data into a larger table of data
        with the same column names and types. Note that this will not work if the
        table to upsert has column names or a column order that does not exactly
        match that of the table it is being upserted into.

        args:
            read_table: the name of the table to read in and use for the upsert
            data_interval_hours: the number of hours to look into the past and read
                (e.g., the last 2 hours)
            write_table: the name of the table we'll be upserting the data into
            write_table_primary_key: the primary key of the write_table
        """
        with self.source_conn.connect() as read_conn:
            write_time = datetime.datetime.now()

            # read from postgres and write the temp table
            delta_query = f"SELECT * FROM {read_table} WHERE updated_at >=\
                (NOW() - INTERVAL'{data_interval_hours} hours')"
            df = pd.read_sql(delta_query, read_conn)

        nrows = df.shape[0]

        self.append_rows_to_table_from_dataframe(
            write_table, write_table_primary_key, df
        )

        # now, as part of the upsert process, remove any rows from our projection that
        # have been removed from the source this is NOT dependent upon there being any
        # new data to upsert
        logging.info(
            f"Upsert process: Initiating row cleanup process for table {write_table}"
        )

        def _get_projection_ids(conn, table):
            res = conn.execute(f"SELECT id FROM {table}")
            employment_ids = set([i[0] for i in res.all()])

            return employment_ids

        def _get_projection_ids_to_remove():
            with self.dest_conn.connect() as dest:
                with self.source_conn.connect() as source:
                    ids = (
                        _get_projection_ids(dest, read_table),
                        _get_projection_ids(source, write_table),
                    )
                    logging.info(
                        f"Upsert process: calculating IDs to remove from {write_table}"
                    )
                    deleted_ids = ids[0].difference(ids[1])

            return deleted_ids

        ids_to_remove = _get_projection_ids_to_remove()
        if len(ids_to_remove) == 0:
            logging.info(
                f"Upsert process: no rows to remove from {read_table} projection."
            )
        else:
            logging.info(
                f"Upsert process: removing {len(ids_to_remove)} source-deleted rows\
                    from {read_table} projection"
            )
            with self.dest_conn.connect() as dest:
                # make SUPER sure we don't delete anything from Heroku PG
                if isinstance(self.dest_conn, HerokuConnection):
                    print("Cannot delete rows from Heroku PG. Exiting.")
                else:
                    dest.execute(
                        f"DELETE FROM {write_table} WHERE id IN\
                            {tuple(ids_to_remove)}".replace(
                            ",)", ")"
                        )
                    )
                    logging.info(
                        f"Upsert process: row clean-up complete for table {write_table}"
                    )

        return json.dumps(
            {
                "table_name": write_table,
                "rows_added": nrows,
                "rows_deleted": len(ids_to_remove),
                "execution_duration": str(datetime.datetime.now() - write_time),
                "execution_date": str(write_time),
            }
        )

    def gen_update_set_parms(
        self, merging_in: str, table_cols: list, primary_key_id: str
    ) -> str:
        """
        Used to generate the parameters for an ON CONFLICT ... UPDATE SET call.

        args:
            merging_in: the name of the table we're merging in
            table_cols: a list of the columns in that table (which must match the
                columns of the table we're merging into)
            primary_key_id: the primary key of the tables we're working with (which
                must be present in both tables)
        """
        update_set_parms = ", ".join(
            [
                f'"{col}" = {merging_in}.{col}'
                for col in table_cols
                if col != primary_key_id
            ]
        )

        return update_set_parms

    def ingest_crunchbase_flatfiles(self) -> str:
        """
        Uses the crunchbase bulk export API endpoint to download Crunchbase's
        daily .tar.gz export of CSVs. The CSVs are then loaded into individual
        tables in Cloud SQL.
        """

        start = datetime.datetime.now()
        output_filepath = "bulk_export.tar.gz"
        url = "https://api.crunchbase.com/bulk/v4/bulk_export.tar.gz"

        print("Downloading bulk_export.tar.gz")
        resp = requests.get(
            url,
            stream=True,
            params={"user_key": self.source_conn.get_secret("crunchbase-api-key")},
        )
        with open(output_filepath, "wb") as f:
            f.write(resp.raw.read())

        with tarfile.open(output_filepath) as tf:
            # get the filenames in the tarball
            names = tf.getnames()

            # extract all the files at once
            extract_path = "bulk_export_extracted"
            tf.extractall(path=extract_path)

        target_tables = [
            "cb_organizations",
            "cb_ipos",
            "cb_acquisitions",
            "cb_funding_rounds",
        ]
        # for each file we extracted, load into to cloud sql
        for name in names:
            # filenames look like organizations.csv
            table_name = f"cb_{name.split('.')[0]}"
            if table_name in target_tables:
                print(f"Writing {table_name} to Cloud SQL")

                # read the csv into pandas
                df = pd.read_csv(f"{extract_path}/{name}")

                # create and write out the table
                self.create_table_from_dataframe(table_name, df)
                self.write_from_dataframe(table_name, df)

                # garbage collect the dataframe to save on memory
                del df
                gc.collect()

        return f"Done writing crunchbase flatfiles in {datetime.datetime.now() - start}"

    def append_rows_to_table_from_dataframe(
        self, write_table: str, write_table_primary_key: str, df: pd.DataFrame
    ) -> str:
        """
        Appends a set of rows from a dataframe to an existing table with the same
        schema. Note that this will not work if the dataframe has column names or
        a column order that does not exactly match that of the table it is being
        appended to.

        args:
            df: dataframe containing the rows to append
            write_table: the name of the table we'll be upserting the data into
            write_table_primary_key: the primary key of the write_table
        """

        nrows = int(df.shape[0])

        if nrows != 0:
            temp_write_table = f"{write_table}_temp"
            dtypes = self.get_postgres_table_schema(write_table)

            self.create_table_from_dataframe(temp_write_table, df, dtypes=dtypes)
            self.write_from_dataframe(temp_write_table, df)

            try:
                with self.dest_conn.connect() as write_conn:
                    table_cols = list(df.columns)
                    write_conn.execute(
                        f"""
                            INSERT INTO {write_table}
                            SELECT * FROM {temp_write_table}
                            ON CONFLICT ({write_table_primary_key}) DO
                                UPDATE SET {self.gen_update_set_parms(
                                    "EXCLUDED", table_cols, write_table_primary_key
                                    )}
                            """
                    )
            except Exception as err:
                raise RuntimeError(f"Error upserting temp table: {err}") from err
            finally:
                self.delete_table(temp_write_table)

        return f"Appending to Cloud SQL table {write_table} complete in for {nrows}."


class HerokuWriter(BaseWriter):
    """
    A Heroku Postgres-specific Writer class with methods that will allow
    the user to write data out to a Heroku-managed postgres instance.
    Note that this class does NOT have methods for creating new tables
    in Heroku Postgres.
    """

    def __init__(
        self,
        source_conn: Union[HerokuConnection, CloudSQLConnection, BQConnection, None],
        dest_conn: HerokuConnection,
    ):
        assert isinstance(dest_conn, HerokuConnection), TypeError(
            "Destination connection type must be a HerokuConnection object."
        )
        super().__init__(source_conn, dest_conn)

    def safe_insert(
        self, table: str, schema: str = "", data: Union[list[dict], dict] = {}
    ):
        """
        Inserts data (one row or many) into a table and fails if the data
        being inserted conflicts with existing data. This depends on the
        table being inserted into having a primary key with a unique
        constraint.

        args:
            table: the name of the table to insert data into
            schema (optional): the schema (e.g., postgres) for the table
            data: the data to insert
        """
        start = datetime.datetime.now()
        if isinstance(data, list):
            cols = list(data[0].keys())
        elif isinstance(data, dict):
            cols = list(data.keys())
        else:
            raise TypeError("Error: data must be a dict or a list of dicts")

        with self.dest_conn.connect() as conn:
            try:
                if schema == "":
                    tbl = sqlalchemy.table(
                        table, *[sqlalchemy.column(col) for col in cols]
                    )

                else:
                    tbl = sqlalchemy.table(
                        table, *[sqlalchemy.column(col) for col in cols], schema=schema
                    )

                conn.execute(sqlalchemy.insert(tbl), data)
            except sqlalchemy.exc.IntegrityError as conflict_err:
                raise RuntimeError(
                    f"Error: conflict on attempted insert: {conflict_err}."
                )

        return f"Safely inserted rows in {datetime.datetime.now() - start}"
