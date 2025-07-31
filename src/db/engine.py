from prefect.blocks.system import Secret

from sqlmodel import Session, create_engine, select
from src.db.models import Offer, Order, OrderItem, Product, Marketplace, PriceHistory, StockHistory, ProductMarketplaceLink  # noqa: F401
from src.db import crud

DB_URL = Secret.load("psql-db-url").get()


engine = create_engine(
    DB_URL,
    pool_size=20,         # default is 5
    max_overflow=0,      # default is 10
    pool_timeout=60, 
)
# engine = create_engine(str(settings.DB_URL), echo=True)      # echo prints SQL – good for tests

def init_db(session: Session) -> None:
    from sqlmodel import SQLModel
    SQLModel.metadata.create_all(engine)
    # Example: Insert initial data if needed

if __name__ == "__main__":
    with Session(engine) as session:
        init_db(session)