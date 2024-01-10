import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

DATABASE_URL = 'postgresql://{}:{}@{}:{}/{}?keepalives_idle=1&keepalives_count=1&tcp_user_timeout=1000'.format(
    os.getenv("DATABASE_USERNAME", ""),
    os.getenv("DATABASE_PASSWORD", ""),
    os.getenv("DATABASE_HOSTNAME", ""),
    os.getenv("DATABSE_PORT", ""),
    os.getenv("DATABASE_NAME", "")
)
engine = create_engine(DATABASE_URL, max_overflow=10,
                       pool_size=20, pool_pre_ping=True, pool_recycle=3600, connect_args={"connect_timeout": 50})
# engine = create_engine(settings.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
db = SessionLocal()
