from datetime import datetime
from pathlib import Path
import re

import pandas as pd

from src.core.config import OUT_DIR
from src.core.logger import get_logger
from src.io.io_excel import get_single_input_file, read_table, write_excel_sheets
from src.jobs.jobPROVERCA_NP import PATTERN_EXTRACT, PATTERN_LIST
from src.processing.transform import protect_from_excel_formulas


logger = get_logger(__name__)


OUTPUT_FILE_BASE_NAME = "result_np_only_in_extract"

COLUMN_PLATES = "План установки пломб"
SHEET_ONLY_IN_EXTRACT = "НП только в выгрузке"


def console_info(message: str) -> None:
    """Печатает короткий статус обработки в консоль."""
    print(f"[jobPROVERCA_NP_SORT_BY_DATE] {message}", flush=True)


def build_output_file_name(base_name: str) -> str:
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_name}_{current_datetime}.xlsx"


def normalize_plate_token(token: str) -> str:
    """Оставляет в номере НП только цифры."""
    return re.sub(r"\D+", "", str(token))


def extract_plates_from_cell(value) -> set[str]:
    """
    Достает все номера НП из одной ячейки.

    Работает с вариантами:
    - 123,456
    - 123/456
    - 123 / 456, 789
    - текст 123; 456
    """
    if pd.isna(value):
        return set()

    text = str(value).strip()
    if not text:
        return set()

    return {
        normalize_plate_token(token)
        for token in re.findall(r"\d+", text)
        if normalize_plate_token(token)
    }


def looks_like_header(value) -> bool:
    """Определяет служебную первую строку списка, например 'Код 1691'."""
    if pd.isna(value):
        return False

    text = str(value).strip()
    if not text:
        return False

    has_letters = bool(re.search(r"[A-Za-zА-Яа-яЁё]", text))
    has_digits = bool(re.search(r"\d", text))
    return has_letters and has_digits


def read_plates_list(file_path: Path) -> set[str]:
    """
    Читает список НП из первого столбца первого листа.

    Если первая непустая ячейка похожа на заголовок, она пропускается.
    В остальных ячейках номера разбираются через любые разделители.
    """
    logger.info(f"Чтение списка НП из файла: {file_path.name}")
    df = read_table(file_path, sheet_name=0, header=None)

    if df.empty:
        return set()

    first_column = df.iloc[:, 0].dropna()
    values = list(first_column)

    if values and looks_like_header(values[0]):
        logger.info(f"Пропущена первая строка списка как заголовок: {values[0]}")
        values = values[1:]

    plates: set[str] = set()
    for value in values:
        plates.update(extract_plates_from_cell(value))

    logger.info(f"В списке НП найдено уникальных номеров: {len(plates)}")
    return plates


def read_extraction_table(file_path: Path) -> pd.DataFrame:
    """Читает выгрузку и проверяет наличие колонки с планом установки пломб."""
    logger.info(f"Чтение выгрузки из файла: {file_path.name}")
    df = read_table(file_path, sheet_name=0)

    if COLUMN_PLATES not in df.columns:
        raise KeyError(f"В выгрузке отсутствует обязательная колонка '{COLUMN_PLATES}'")

    return df


def extract_row_plates(row: pd.Series) -> set[str]:
    """Извлекает все нормализованные НП из строки выгрузки."""
    return extract_plates_from_cell(row.get(COLUMN_PLATES))


def build_only_in_extract_df(extraction_df: pd.DataFrame, list_plates: set[str]) -> pd.DataFrame:
    """
    Возвращает полные данные выгрузки по каждому НП, которого нет в списке о НП.

    Важно: одна строка результата = один несовпавший НП.
    Если в одной строке выгрузки несколько лишних НП, строка выгрузки будет
    повторена для каждого такого номера. Так результат нельзя перепутать
    с количеством исходных строк.
    """
    rows = []

    for _, row in extraction_df.iterrows():
        row_plates = extract_row_plates(row)
        if not row_plates:
            continue

        only_in_extract = sorted(row_plates - list_plates)
        if not only_in_extract:
            continue

        for plate in only_in_extract:
            row_data = row.copy()
            row_data["НП только в выгрузке"] = plate
            row_data["Все НП в строке"] = ", ".join(sorted(row_plates))
            rows.append(row_data)

    if not rows:
        return pd.DataFrame(columns=[*extraction_df.columns, "НП только в выгрузке", "Все НП в строке"])

    return pd.DataFrame(rows)


def run():
    try:
        logger.info("Начало выполнения jobPROVERCA_NP_SORT_BY_DATE")
        console_info("Старт: ищем НП из выгрузки, которых нет в списке о НП.")

        list_file = get_single_input_file(PATTERN_LIST)
        extract_file = get_single_input_file(PATTERN_EXTRACT)
        console_info(f"Список НП: {list_file.name}")
        console_info(f"Выгрузка: {extract_file.name}")

        list_plates = read_plates_list(list_file)
        extraction_df = read_extraction_table(extract_file)

        console_info(f"В списке НП уникальных номеров после нормализации: {len(list_plates)}")
        console_info(f"Строк в выгрузке: {len(extraction_df)}")

        df_only_in_extract = build_only_in_extract_df(extraction_df, list_plates)
        unique_missing_count = (
            df_only_in_extract["НП только в выгрузке"].nunique()
            if not df_only_in_extract.empty
            else 0
        )
        console_info(
            f"Несовпавших НП из выгрузки: {len(df_only_in_extract)} строк результата, "
            f"{unique_missing_count} уникальных номеров"
        )

        output_file = OUT_DIR / build_output_file_name(OUTPUT_FILE_BASE_NAME)
        sheets = {
            SHEET_ONLY_IN_EXTRACT: protect_from_excel_formulas(df_only_in_extract),
        }
        write_excel_sheets(sheets, output_file, index=False)

        logger.info(f"Результат записан в {output_file}")
        console_info(f"Готово. Результат сохранен: {output_file}")

    except Exception as error:
        logger.exception(f"Ошибка при выполнении job: {error}")
        console_info(f"Ошибка: {error}")
        raise


if __name__ == "__main__":
    run()
