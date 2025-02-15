import abc
from typing import TypeVar

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

from config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
from models import Base, SpimexTrade

T = TypeVar("T", bound=Base)


class Repository(abc.ABC):
    """Интерфейс для репозитория"""

    @abc.abstractmethod
    def add(self, entity: list[SpimexTrade]):
        raise NotImplementedError
    

    @abc.abstractmethod
    def update(self, id_: id):
        raise NotImplementedError
    

    @abc.abstractmethod
    def remove(self, id_: id):
        raise NotImplementedError
    

    @abc.abstractmethod
    def get_by_id(self, id_: id):
        raise NotImplementedError


class SQLAlchemyRepository(Repository):
    """Репозиторий для синхронного подключения к БД"""

    def __init__(self) -> None:
        self.engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}", echo=False)
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        self.new_session = sessionmaker(self.engine, expire_on_commit=False)

    def add(self, entity: list[T]) -> None:
        """Добавляет список объектов в БД

        Args:
            entity (list[T]): Список объектов для добавления
        """
        with self.new_session() as s:
            s.add_all(entity)
            s.commit()

    def update(self, id_: id):
        ...
    
    def remove(self, id_: id):
        ...

    def get_by_id(self, id_: id):
        ...


class SQLAlchemyRepositoryAsync(Repository):
    """Репозиторий для асинхронного подключения к БД"""

    def __init__(self) -> None:
        self.async_engine = create_async_engine(f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}", echo=False)
        self.new_session = async_sessionmaker(self.async_engine, expire_on_commit=False)

    async def init_models(self):
        async with self.async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
            await conn.commit()

    async def add(self, entity: list[T]) -> None:
        """Добавляет список объектов в БД

        Args:
            entity (list[T]): Список объектов для добавления
        """
        async with self.new_session() as s:
            s.add_all(entity)
            await s.commit()

    async def update(self, id_: id):
        ...
    
    async def remove(self, id_: id):
        ...

    async def get_by_id(self, id_: id):
        ...