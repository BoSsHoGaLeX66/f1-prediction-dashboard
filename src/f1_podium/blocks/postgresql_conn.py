from prefect.blocks.core import Block
from pydantic import SecretStr

try:
    from f1_podium.db.connection import create_postgres_engine
except ModuleNotFoundError:  # running as a script
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from f1_podium.db.connection import create_postgres_engine


class PostgresqlConnector(Block):
    user: str
    db_name: str
    host: str
    password: SecretStr
    port: int

    def get_engine(self):
        """Return a SQLAlchemy engine for this block's configured database."""
        return create_postgres_engine(
            self.user,
            self.password.get_secret_value(),
            self.host,
            self.port,
            self.db_name,
        )


if __name__ == "__main__":
    PostgresqlConnector.register_type_and_schema()
