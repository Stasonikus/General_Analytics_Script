from pathlib import Path
import pandas as pd

from src.io_excel import read_excel, read_table


def read_input_file(file_path: Path, sheet_name=0) -> pd.DataFrame:
    if file_path.suffix.lower() == ".csv":
        return read_table(file_path)

    return read_excel(file_path, sheet_name=sheet_name)


def add_source_file_column(
    df: pd.DataFrame,
    source_file_name: str,
    column_name: str = "source_file",
) -> pd.DataFrame:
    result = df.copy()
    result[column_name] = source_file_name
    return result