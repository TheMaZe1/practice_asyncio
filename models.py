from sqlalchemy import Column, Date, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase



class Base(DeclarativeBase):
    pass


class SpimexTrade(Base):
    __tablename__ = 'spimex_trading_results'

    id_ = Column(Integer, primary_key=True)
    exchange_product_id = Column(String)
    exchange_product_name = Column(String)
    oil_id = Column(String)
    delivery_basis_id = Column(String)
    delivery_basis_name = Column(String)
    delivery_type_id = Column(String)
    volume = Column(Integer)
    total = Column(Float)
    count = Column(Integer)
    date = Column(Date)
    created_on = Column(Date)
    updated_on = Column(Date)