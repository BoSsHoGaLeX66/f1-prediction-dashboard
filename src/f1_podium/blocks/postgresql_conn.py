from prefect.blocks.core import Block
from sqlalchemy import create_engine
from pydantic import SecretStr


class PostgresqlConnector(Block):
    user: str
    db_name: str
    host: str
    password: SecretStr
    port: int

    def get_engine(self):
        return create_engine(
            f"postgresql://{self.user}:{self.password.get_secret_value()}@{self.host}:{self.port}/{self.db_name}"
        )


if __name__ == "__main__":
    PostgresqlConnector.register_type_and_schema()

