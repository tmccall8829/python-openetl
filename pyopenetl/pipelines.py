from typing import Union

import pyopenetl.connections as poe_conns
import pyopenetl.operations as poe_ops


class BasePipeline:
    """
    A basic Pipeline class that opens a connection between a read connection object
    and a write connection object.

    args:
        read_conn: the connection object for reading from the data source
        write_conn: the connection object for writing to the data destination
    """

    def __init__(
        self,
        read_conn: Union[
            poe_conns.BQConnection,
            poe_conns.CloudSQLConnection,
            poe_conns.HerokuConnection,
        ],
        write_conn: poe_conns.CloudSQLConnection,
    ) -> None:
        self.write_conn = write_conn
        self.read_conn = read_conn

        if isinstance(self.write_conn, poe_conns.CloudSQLConnection):
            self.output = poe_ops.CloudSQLWriter(self.read_conn, self.write_conn)
        else:
            raise TypeError(f"Error: unsupported pipeline output: {self.write_conn}")


class SeedPipeline(BasePipeline):
    """
    Opens a connection between a source and destination and seeds a table
    from the source to the destination.

    args:
        read_conn: the read connection object (the place we're reading data from)
        write_conn: the place we're seeding the table
    """

    def __init__(
        self,
        read_conn: Union[
            poe_conns.BQConnection,
            poe_conns.CloudSQLConnection,
            poe_conns.HerokuConnection,
        ],
        write_conn: poe_conns.CloudSQLConnection,
    ) -> None:
        super().__init__(read_conn, write_conn)

    def execute(self, read_table: str, write_table: str, read_chunksize: int = 100000):
        """
        Starts the table seeding process using the source and destination connections.

        args:
            read_table: the name of the table to read from
            write_table: the name of the table to write out
            read_chunksize: the number of rows to seed at a time
        """

        return self.output.seed_table(
            read_table=read_table,
            read_chunksize=read_chunksize,
            write_table=write_table,
        )


class UpdatePipeline(BasePipeline):
    """
    Opens a connection between a source and destination and upserts a table
    from the source to the destination.

    args:
        read_conn: the read connection object (the place we're reading data from)
        write_conn: the place we're upserting the table
    """

    def __init__(
        self,
        read_conn: Union[
            poe_conns.BQConnection,
            poe_conns.CloudSQLConnection,
            poe_conns.HerokuConnection,
        ],
        write_conn: poe_conns.CloudSQLConnection,
    ) -> None:
        super().__init__(read_conn, write_conn)

    def execute(
        self,
        read_table: str,
        write_table: str,
        data_interval_hours: int = 1,
        primary_key: str = "id",
    ):
        """
        Starts the table upserting process using the source and destination connections.

        args:
            read_table: the name of the table to read from
            write_table: the name of the table to write out
            data_interval_hours: the number of previous hours' worth of data to upsert
            primary_key: the primary key of the read and write tables (must match)
        """
        try:
            return self.output.update_table_via_upsert(
                read_table=read_table,
                data_interval_hours=data_interval_hours,
                write_table=write_table,
                write_table_primary_key=primary_key,
            )
        except Exception as err:
            return f"Unable to update table {read_table} as {write_table}: {err}"


class CrunchbaseFlatfilePipeline(BasePipeline):
    def __init__(
        self,
        read_conn: poe_conns.BaseConnection,
        write_conn: poe_conns.CloudSQLConnection,
    ) -> None:
        super().__init__(read_conn=read_conn, write_conn=write_conn)

    def execute(self) -> str:
        return self.output.ingest_crunchbase_flatfiles()
