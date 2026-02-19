from sqlalchemy import create_engine

MYSQL_USER = "root"
MYSQL_PASSWORD = "1234"
MYSQL_HOST = "host.docker.internal"   # IMPORTANT for Codespaces later
MYSQL_PORT = "3306"
MYSQL_DB = "public_transport_dw"

engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}",
    pool_pre_ping=True
)
engine.connect()
