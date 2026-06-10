from typing import Iterable, Optional

import pandas as pd


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Нормализует названия колонок:
    - убирает пробелы по краям
    - заменяет переводы строк
    - схлопывает двойные пробелы
    """
    result = df.copy()

    result.columns = [
        " ".join(str(col).replace("\n", " ").strip().split())
        for col in result.columns
    ]
    return result


def strip_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Убирает лишние пробелы во всех текстовых колонках.
    """
    result = df.copy()

    for col in result.columns:
        if pd.api.types.is_object_dtype(result[col]):
            result[col] = result[col].apply(
                lambda x: x.strip() if isinstance(x, str) else x
            )

    return result


def add_source_column(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """
    Добавляет колонку с именем источника.
    Полезно при объединении нескольких файлов.
    """
    result = df.copy()
    result["source_file"] = source_name
    return result


def select_columns(
    df: pd.DataFrame,
    columns: Iterable[str],
    required: bool = True,
) -> pd.DataFrame:
    """
    Оставляет только указанные колонки.

    required=True:
        если хотя бы одной колонки нет — ошибка.
    """
    result = df.copy()
    columns = list(columns)

    missing = [col for col in columns if col not in result.columns]
    if missing and required:
        raise KeyError(f"Отсутствуют колонки: {missing}")

    existing = [col for col in columns if col in result.columns]
    return result[existing]


def rename_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """
    Переименовывает колонки по словарю.
    """
    return df.rename(columns=mapping).copy()


def concat_dataframes(
    frames: list[pd.DataFrame],
    ignore_index: bool = True,
) -> pd.DataFrame:
    """
    Объединяет список DataFrame в один.
    """
    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=ignore_index)


def basic_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Базовая типовая очистка для большинства задач.
    """
    result = df.copy()
    result = normalize_columns(result)
    result = strip_text_columns(result)
    return result


def split_dataframe_by_column(
    df: pd.DataFrame,
    column_name: str,
) -> dict[str, pd.DataFrame]:
    """
    Разбивает DataFrame на словарь DataFrame по значениям колонки.

    Пример:
        {'A': df_a, 'B': df_b}
    """
    if column_name not in df.columns:
        raise KeyError(f"Колонка не найдена: {column_name}")

    result = {}
    for value, part in df.groupby(column_name, dropna=False):
        sheet_name = "EMPTY" if pd.isna(value) else str(value)
        result[sheet_name] = part.copy()

    return result


def filter_rows(
    df: pd.DataFrame,
    column_name: str,
    allowed_values: Optional[Iterable] = None,
) -> pd.DataFrame:
    """
    Фильтрует строки по допустимым значениям колонки.
    """
    if column_name not in df.columns:
        raise KeyError(f"Колонка не найдена: {column_name}")

    if allowed_values is None:
        return df.copy()

    allowed_values = set(allowed_values)
    return df[df[column_name].isin(allowed_values)].copy()

def protect_from_excel_formulas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Защищает текстовые значения от интерпретации Excel как формул.
    Если строка начинается с =, +, - или @, добавляет ведущий апостроф.
    """
    result = df.copy()

    def safe_excel_text(value):
        if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
            return "'" + value
        return value

    for col in result.columns:
        if pd.api.types.is_object_dtype(result[col]):
            result[col] = result[col].apply(safe_excel_text)

    return result