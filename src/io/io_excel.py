from pathlib import Path
from typing import Dict, Iterable, Union

import pandas as pd

from src.core.config import DEFAULT_READ_ENGINE, DEFAULT_WRITE_ENGINE, IN_DIR


PathLike = Union[str, Path]


def ensure_parent_dir(file_path: PathLike) -> None:
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)


def find_input_files(
    patterns: list[str] | tuple[str, ...],
    input_dir: PathLike = IN_DIR,
) -> list[Path]:
    """
    Ищет в папке input_dir файлы по переданным маскам.

    Примеры patterns:
        ["*.xlsx", "*.xls", "*.csv"]
        ["test*.xlsx", "test*.xls", "test*.csv"]
        ["stat_2026_03.*"]

    Возвращает отсортированный список Path без дублей.

    Ошибки:
    - если папка не найдена -> FileNotFoundError
    - если ничего не найдено -> FileNotFoundError
    """
    input_dir = Path(input_dir)

    if not input_dir.exists():
        raise FileNotFoundError(f"Папка входных файлов не найдена: {input_dir}")

    if not patterns:
        raise ValueError("Не заданы маски поиска входных файлов")

    found_files: list[Path] = []

    for pattern in patterns:
        found_files.extend(input_dir.glob(pattern))

    files = sorted(set(found_files), key=lambda p: p.name.lower())

    if not files:
        patterns_text = ", ".join(patterns)
        raise FileNotFoundError(
            f"В папке '{input_dir}' не найдено файлов по маскам: {patterns_text}"
        )

    return files


def get_single_input_file(
    patterns: list[str] | tuple[str, ...],
    input_dir: PathLike = IN_DIR,
) -> Path:
    """
    Возвращает ровно один входной файл по маске/маскам.

    Логика:
    - 0 файлов -> ошибка
    - 1 файл -> вернуть его
    - много файлов -> ошибка со списком
    """
    files = find_input_files(patterns=patterns, input_dir=input_dir)

    if len(files) == 1:
        return files[0]

    file_list = "\n".join(f" - {file.name}" for file in files)
    patterns_text = ", ".join(patterns)

    raise FileNotFoundError(
        "Найдено несколько входных файлов, "
        "а для текущей обработки ожидается один файл.\n"
        f"Маски: {patterns_text}\n"
        f"Файлы:\n{file_list}"
    )


def read_table(
    file_path: PathLike,
    sheet_name=0,
    dtype=None,
    header=0,
    encoding="utf-8",
    sep=None,
) -> pd.DataFrame:
    """
    Универсальное чтение таблицы:
    - Excel (.xlsx, .xls)
    - CSV (.csv)

    Для Excel используется sheet_name.
    Для CSV sheet_name игнорируется.

    sep:
    - None -> сначала попытка стандартного чтения CSV,
      затем fallback на ';'
    - иначе используется указанный разделитель
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    suffix = file_path.suffix.lower()

    if suffix in [".xlsx", ".xls"]:
        return pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            dtype=dtype,
            header=header,
            engine=DEFAULT_READ_ENGINE,
        )

    if suffix == ".csv":
        if sep is not None:
            return pd.read_csv(
                file_path,
                dtype=dtype,
                header=header,
                encoding=encoding,
                sep=sep,
            )

        try:
            return pd.read_csv(
                file_path,
                dtype=dtype,
                header=header,
                encoding=encoding,
            )
        except Exception:
            return pd.read_csv(
                file_path,
                dtype=dtype,
                header=header,
                encoding=encoding,
                sep=";",
            )

    raise ValueError(f"Неподдерживаемый формат файла: {file_path}")


def read_excel(
    file_path: PathLike,
    sheet_name=0,
    dtype=None,
    header=0,
) -> pd.DataFrame:
    """
    Чтение одного Excel-листа.
    Оставлено для совместимости со старым кодом.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    return pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        dtype=dtype,
        header=header,
        engine=DEFAULT_READ_ENGINE,
    )


def read_excel_all_sheets(
    file_path: PathLike,
    dtype=None,
    header=0,
) -> Dict[str, pd.DataFrame]:
    """
    Чтение всех листов Excel-файла.
    Только для .xlsx / .xls.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix not in [".xlsx", ".xls"]:
        raise ValueError(
            f"Чтение всех листов поддерживается только для Excel-файлов: {file_path}"
        )

    return pd.read_excel(
        file_path,
        sheet_name=None,
        dtype=dtype,
        header=header,
        engine=DEFAULT_READ_ENGINE,
    )


def read_many_excel(
    files: Iterable[PathLike],
    sheet_name=0,
    dtype=None,
    header=0,
) -> Dict[str, pd.DataFrame]:
    """
    Чтение нескольких Excel-файлов.
    Только для .xlsx / .xls.
    """
    result = {}

    for file_path in files:
        path = Path(file_path)
        result[path.name] = read_excel(
            path,
            sheet_name=sheet_name,
            dtype=dtype,
            header=header,
        )

    return result


def read_many_tables(
    files: Iterable[PathLike],
    sheet_name=0,
    dtype=None,
    header=0,
    encoding="utf-8",
    sep=None,
) -> Dict[str, pd.DataFrame]:
    """
    Читает несколько файлов таблиц:
    - Excel (.xlsx, .xls)
    - CSV (.csv)

    Возвращает словарь:
    {
        "file1.xlsx": DataFrame,
        "file2.csv": DataFrame,
    }
    """
    result = {}

    for file_path in files:
        path = Path(file_path)
        result[path.name] = read_table(
            path,
            sheet_name=sheet_name,
            dtype=dtype,
            header=header,
            encoding=encoding,
            sep=sep,
        )

    return result


def write_excel(
    df: pd.DataFrame,
    file_path: PathLike,
    sheet_name: str = "Sheet1",
    index: bool = False,
) -> None:
    ensure_parent_dir(file_path)

    with pd.ExcelWriter(file_path, engine=DEFAULT_WRITE_ENGINE) as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=index)


def write_excel_sheets(
    sheets: Dict[str, pd.DataFrame],
    file_path: PathLike,
    index: bool = False,
) -> None:
    ensure_parent_dir(file_path)

    with pd.ExcelWriter(file_path, engine=DEFAULT_WRITE_ENGINE) as writer:
        for sheet_name, df in sheets.items():
            safe_sheet_name = str(sheet_name)[:31]
            df.to_excel(writer, sheet_name=safe_sheet_name, index=index)