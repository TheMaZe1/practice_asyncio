import os
import time
import datetime
from calendar import monthrange

import requests
import xlrd

from database import Repository, SQLAlchemyRepository
from models import SpimexTrade


class SyncParser:
    """Класс парсит данные отчетов за указзаный год, начиная с определенного месяца и дня. Сохраняет их в БД"""

    BASE_LINK = 'https://spimex.com/upload/reports/oil_xls/'
    SAVE_DIR = 'reports_oil'

    def __init__(self, year: int, repo: Repository, start_month: int=1, start_day: int=1) -> None:
        self.year: int = year
        self.start_month: int = start_month
        self.start_day: int = start_day
        self.repo = repo
        os.makedirs(self.SAVE_DIR, exist_ok=True)

    def _download_file(self, date: datetime.date) -> None:
        """Скачивает файл отчета за указанную дату."""
        str_date: datetime.date = date.strftime('%Y%m%d')
        filename: str = 'oil_xls_' + f'{str_date}162000.xls'
        response = requests.get(self.BASE_LINK + filename)
        if response.status_code == 404:
            return
        with open(os.path.join(self.SAVE_DIR, filename), 'wb') as f:
            f.write(response.content)

    def _downloads_all_files(self) -> None:
        """Запускает скачивание файлов за указанный период."""
        for month in range(self.start_month, 13):
            for day in range(self.start_day, monthrange(self.year, month)[1] + 1):
                date: datetime = datetime.date(self.year, month, day)
                self._download_file(date)

    @classmethod
    def _map_row_to_spimex_trade(cls, row: list, date: datetime) -> SpimexTrade:
        """Конвертирует строку XLS файла с данными в объект для сохранения в БД"""
        return SpimexTrade(
            exchange_product_id = row[1].value,
            exchange_product_name = row[2].value,
            oil_id = row[1].value[:4],
            delivery_basis_id = row[1].value[4:7],
            delivery_basis_name = row[3].value,
            delivery_type_id = row[1].value[-1],
            volume = row[4].value,
            total = row[5].value,
            count = row[-1].value,
            date = date,
            created_on = datetime.datetime.now(),
            updated_on = datetime.datetime.now()

    )

    def _save_file(self, filename: str) -> None:
        """Обрабатывает и созраняет файл в БД"""
        file_path = os.path.join(self.SAVE_DIR, filename)

        try:
            date: datetime = datetime.datetime.strptime(filename.split('_')[-1][:8], '%Y%m%d')
            start_row: int = 0
            spimex_trades: list[SpimexTrade] = []
            
            wb = xlrd.open_workbook(file_path)
            ws = wb.sheet_by_index(0)
            
            for row in range(ws.nrows):                             # Ищем на какой строчке начинаются данные
                if ws.row(row)[1].value == 'Единица измерения: Метрическая тонна':
                    start_row = row + 3                              # Данные начинаются на 2 строчки ниже строки 'Единица измерения: Метрическая тонна'
                    break
            else:
                return #Если не нашли нужную строку, пропускаем файл

            while ws.row(start_row)[1].value != 'Итого:':
                if ws.row(start_row)[-1].value != '-':
                    spimex_trades.append(self._map_row_to_spimex_trade(ws.row(start_row), date))
                start_row += 1
            self.repo.add(spimex_trades)

        except Exception as e:
            print(f"Ошибка при обработке {filename}: {e}")

    def _save_all_files(self) -> None:
        """Обрабатывает и сохраняет данные из всех файлов"""

        for filename in os.listdir(self.SAVE_DIR):
            self._save_file(filename)

    def parse(self) -> None:
        """Парсит данные отчета за каждый день в заданном году, если отчет существует сохраняет в БД"""
        self._downloads_all_files()
        self._save_all_files()
    

if __name__ == '__main__':
    sqlrepo = SQLAlchemyRepository()
    start_time = time.time()
    p = SyncParser(2023, sqlrepo, 12, 20,)
    p.parse()
    end_time = time.time()
    print(end_time - start_time)
