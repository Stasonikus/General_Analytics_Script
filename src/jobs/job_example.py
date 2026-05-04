import pandas as pd

from src.config import OUT_DIR
from src.io_excel import get_single_input_file, read_excel, write_excel_sheets
from src.transform import basic_cleanup, protect_from_excel_formulas
from src.logger import get_logger
from datetime import datetime


logger = get_logger(__name__)


INPUT_PATTERNS = ["test*.xlsx", "test*.xls", "test*.csv"]

#OUTPUT_FILE_NAME = "result_job_example.xlsx"
OUTPUT_FILE_BASE_NAME = "result_job_example"
SOURCE_COLUMN_NAME = "source_file"

INPUT_SHEET_NAME = 0
OUTPUT_SHEET_1_NAME = "лист1"
OUTPUT_SHEET_2_NAME = "лист2"

COLUMN_TRANSPORT = "Вид транспорта при транзите"
COLUMN_PRODUCT_CODE = "Код товара"

FILTER_TRANSPORT_VALUE = "20"
FILTER_PRODUCT_CODES = {"2204", "2205", "8521", "8525"}


def build_output_file_name(base_name: str) -> str:
    current_date = datetime.now().strftime("%Y%m%d")
    return f"{base_name}_{current_date}.xlsx"

def validate_input_columns(df: pd.DataFrame) -> None:
    required_columns = {COLUMN_TRANSPORT, COLUMN_PRODUCT_CODE}
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise KeyError(
            f"В исходной таблице отсутствуют обязательные колонки: {missing}"
        )


def prepare_dataframe(df: pd.DataFrame, source_file_name: str) -> pd.DataFrame:
    result = df.copy()
    result = basic_cleanup(result)

    validate_input_columns(result)

    result[COLUMN_TRANSPORT] = result[COLUMN_TRANSPORT].astype(str).str.strip()
    result[COLUMN_PRODUCT_CODE] = result[COLUMN_PRODUCT_CODE].astype(str).str.strip()

    result[SOURCE_COLUMN_NAME] = source_file_name

    return result


def filter_result(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    result = result[result[COLUMN_TRANSPORT] == FILTER_TRANSPORT_VALUE].copy()
    result = result[result[COLUMN_PRODUCT_CODE].isin(FILTER_PRODUCT_CODES)].copy()

    return result


def run():
    input_file = get_single_input_file(INPUT_PATTERNS)
    output_file_name = build_output_file_name(OUTPUT_FILE_BASE_NAME)
    output_file = OUT_DIR / output_file_name

    logger.info(f"Найден входной файл: {input_file.name}")

    if input_file.suffix.lower() == ".csv":
        from src.io_excel import read_table
        df_sheet_1 = read_table(input_file)
    else:
        df_sheet_1 = read_excel(input_file, sheet_name=INPUT_SHEET_NAME)

    logger.info(
        f"Лист 1 прочитан: строк={len(df_sheet_1)}, "
        f"колонок={len(df_sheet_1.columns)}"
    )

    prepared_df = prepare_dataframe(df_sheet_1, input_file.name)
    result_df = filter_result(prepared_df)

    logger.info(f"После фильтрации строк: {len(result_df)}")

    sheets_to_write = {
        OUTPUT_SHEET_1_NAME: prepared_df,
        OUTPUT_SHEET_2_NAME: result_df,
    }

    write_excel_sheets(
        sheets=sheets_to_write,
        file_path=output_file,
        index=False,
    )

    logger.info(f"Результат записан: {output_file}")