from sqlmodel import Session, create_engine, select
from src.db.models import Offer, Order, OrderItem, Product, Marketplace, PriceHistory, StockHistory, ProductMarketplaceLink  # noqa: F401
from src.db import crud



engine = create_engine(str(DB_URL))      # echo prints SQL – good for tests
# engine = create_engine(str(settings.DB_URL), echo=True)      # echo prints SQL – good for tests

def init_db(session: Session) -> None:
    from sqlmodel import SQLModel
    SQLModel.metadata.create_all(engine)

    # Example: Insert initial data if needed


if __name__ == "__main__":
    with Session(engine) as session:
        init_db(session)