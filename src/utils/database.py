# db_connection.py
from sqlalchemy import create_engine, inspect
from utils.config import DB_HOST, DB_USERNAME, DB_PASSWORD, DB_PORT
from sqlalchemy.sql import text

class Database:
    def __init__(self, database):
        """
        Initialize the DatabaseConnection class with database credentials.

        Parameters:
        username (str): Database username
        password (str): Database password
        host (str): Database host
        database (str): Database name
        port (int): Database port, default is 3306
        """
        self.username = DB_USERNAME
        self.password = DB_PASSWORD
        self.host = DB_HOST
        self.database = database
        self.port = DB_PORT

        self.engine = create_engine(f'mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}')

    def get_engine(self):
        """
        Return the SQLAlchemy engine.

        Returns:
        sqlalchemy.engine.base.Engine: SQLAlchemy engine
        """
        return self.engine

    def create_index(self, table_name, columns):
        """
        Create a BTREE index on the specified columns of a table.

        Parameters:
        table_name (str): Name of the table on which to create the index.
        columns (list of str): List of column names to include in the index.
        """
        if not columns:
            raise ValueError("No columns provided for index creation")

        index_name = f"idx_{'_'.join(columns)}"
        column_list = ', '.join(columns)
        sql = text(f"CREATE INDEX `{index_name}` on `{table_name}` ({column_list}) USING BTREE")
        # ADD INDEX `idx_precincts-2023-10-05_district_link` (`district_link`) USING BTREE;

        with self.engine.connect() as connection:
            # connection.execute(text(f"ALTER TABLE `{self.database}`.`{table_name}`"))
            connection.execute(sql)

    def create_view(self, view_name, table_name):
        """
        Create a view with all fields from the specified table.

        Parameters:
        view_name (str): Name of the view to be created.
        table_name (str): Name of the table to create the view from.
        """
        sql_command = text(f"CREATE OR REPLACE VIEW `{view_name}` AS SELECT * FROM `{table_name}`")

        with self.engine.connect() as connection:
            connection.execute(sql_command)

    def create_trigger(self, trigger_statement):
        """
        Create a trigger on the specified table.

        Parameters:
        trigger_name (str): Name of the trigger to be created.
        table_name (str): Name of the table to create the trigger on.
        action (str): Action that triggers the trigger (e.g. BEFORE INSERT).
        statement (str): SQL statement to be executed when the trigger is triggered.
        """
        sql_command = text(trigger_statement)

        with self.engine.connect() as connection:
            connection.execute(sql_command)

    def table_exists(self, table_name):
            """
            Check if a table exists in the database.

            Parameters:
            table_name (str): Name of the table to check.

            Returns:
            bool: True if table exists, False otherwise.
            """
            inspector = inspect(self.engine)
            return inspector.has_table(table_name)
