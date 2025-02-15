
import time

from async_parser import AsyncParser
from database import SQLAlchemyRepository, SQLAlchemyRepositoryAsync
from sync_parser import SyncParser


if __name__ == '__main__':
    sqlrepo = SQLAlchemyRepository()
    start_time = time.time()
    p = SyncParser(2023, sqlrepo, 12, 1)
    p.parse()
    end_time = time.time()
    print("Время работы синхронного парсера: ", end_time - start_time)

    sqlrepo = SQLAlchemyRepositoryAsync()
    start_time = time.time()
    p = AsyncParser(2023, sqlrepo, 12, 1)
    p.parse()
    end_time = time.time()
    print("Время работы асинхронного парсера: ", end_time - start_time)