import contextlib
import os
from typing import Generator

from google.cloud import bigquery, secretmanager
import sqlalchemy


class BaseConnection:
    """
    A basic class for connecting to databases that is inherited by
    all other Connections classes.

    args:
        project (str): The google cloud project we want to use. This is
            used primarily by the get_secret() method to read secrets
            from secret manager.
    """

    def __init__(self, project: str) -> None:
        self.project = project

    def get_secret(self, secret_name: str) -> str:
        """
        Reads a given secret from a GCP project's Secret Manager API. Defaults
        to reading the latest version of the secret.

        args:
            secret_name (str): The full name of the secret being accessed.
        """

        sm_client = secretmanager.SecretManagerServiceClient()
        secret = sm_client.access_secret_version(
            request=secretmanager.AccessSecretVersionRequest(
                name=f"projects/{self.project}/secrets/{secret_name}/versions/latest"
            )
        ).payload.data.decode("utf-8")

        return secret


class PostgresConnection(BaseConnection):
    """
    Connect to a generic postgres instance.
        stream_results: whether or not the SQLAlchemy engine object should add the
            stream_results execution option. this should only be enabled when you
            are reading from a database -- trying to write with this option set
            will result in a somewhat cryptic cursor-related error.
    """

    def __init__(
        self,
        username: str = "postgres",
        password: str = "",
        port: int = 5432,
        db: str = "postgres",
        stream_results: bool = False,
    ) -> None:
        self.instance_ip = os.environ.get("POSTGRES_INSTANCE_IP", "127.0.0.1")
        self.instance_port = port
        self.instance_username = username
        self.instance_password = password
        self.instance_db = db
        self.stream_results = stream_results

    @contextlib.contextmanager
    def connect(self) -> Generator[sqlalchemy.engine.Engine, None, None]:
        """Actually connects to a generic postgresql instance."""

        connection = (
            sqlalchemy.create_engine(
                sqlalchemy.engine.url.URL.create(
                    drivername="postgresql+psycopg2",
                    username=self.instance_username,
                    password=self.instance_password,
                    host=self.instance_ip,
                    port=self.instance_port,
                    database=self.instance_db,
                )
            )
            .connect()
            .execution_options(stream_results=self.stream_results)
        )

        yield connection

        connection.close()


class HerokuConnection(BaseConnection):
    """
    Opens up a connection to a Heroku-managed Postgres instance.

    args:
        project (str): the name of the cloud environment project (GCP, AWS, etc.)
            to map to the Heroku Postgres instance name.
        stream_results: whether or not the SQLAlchemy engine object should add the
            stream_results execution option. this should only be enabled when you
            are reading from a database -- trying to write with this option set
            will result in a somewhat cryptic cursor-related error.
        db_url_secret_name: the name of the cloud environment secret to read the
            postgres database URL from.
    """

    def __init__(
        self,
        project: str,
        stream_results: bool = True,
        db_url_secret_name: str = "heroku-pg-db-url",
    ) -> None:
        super().__init__(project)

        self.stream_results = stream_results
        self.db_url_secret_name = db_url_secret_name

        if self.stream_results:
            self.db_url = self.get_secret(f"{self.db_url_secret_name}-follower")
        else:
            self.db_url = self.get_secret(f"{self.db_url_secret_name}-leader")

    @contextlib.contextmanager
    def connect(self) -> Generator[sqlalchemy.engine.Engine, None, None]:
        """
        Connects to a Heroku postgres environment instance.
        """

        connection = (
            sqlalchemy.create_engine(self.db_url)
            .connect()
            .execution_options(stream_results=self.stream_results)
        )

        yield connection

        connection.close()


class CloudSQLConnection(PostgresConnection):
    """
    Opens up a connection to a GCP Cloud SQL instance.

    args:
        project: the name of the google cloud project to connect to.
        username: username to use to connect to the instance
        gcp_password_secret: the name of the GCP secret manager secret
            containing the password associated with the given username.
            NOT the actual password.
        port: the Cloud SQL instance's port
        db: the database to connect to (often just "postgres")
    """

    def __init__(
        self,
        project: str,
        username: str = "postgres",
        gcp_password_secret: str = "cloudsql_postgres_default_password",
        port: int = 5432,
        db: str = "postgres",
    ) -> None:
        super().__init__(project)

        self.project = project
        self.instance_username = username

        # set the PostgresConnection class's password to be the value
        # of the secret in GCP
        self.instance_password_gcp_secret_name = gcp_password_secret
        self.instance_password = self.get_secret(self.instance_password_gcp_secret_name)

        # use CLOUD_SQL_INSTANCE_IP as the env var instead of
        # POSTGRES_INSTANCE_IP
        self.instance_ip = os.environ.get("CLOUD_SQL_INSTANCE_IP", "127.0.0.1")
        self.instance_port = port
        self.instance_db = db


class BQConnection(BaseConnection):
    """
    Connects to BigQuery using the bigquery client library.

    args:
        project (str): The GCP project containing the BigQuery instance we want
            to connect to.
    """

    def __init__(self, project: str) -> None:
        super().__init__(project)

    @contextlib.contextmanager
    def connect(self) -> Generator[bigquery.Client, None, None]:
        client = bigquery.Client()

        yield client

        client.close()
