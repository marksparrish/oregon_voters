# db_connection.py
from sqlalchemy import create_engine
from utils.config import DB_HOST, DB_USERNAME, DB_PASSWORD, DB_PORT

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
