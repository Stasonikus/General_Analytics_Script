from datetime import datetime

import pandas as pd

from src.core.config import OUT_DIR
from src.io.io_excel import get_single_input_file, read_excel, read_table, write_excel_sheets
from src.processing.transform import basic_cleanup, protect_from_excel_formulas
from src.core.logger import get_logger


logger = get_logger(__name__)


INPUT_PATTERNS = [
    "ТО отправления*.xlsx",
    "ТО отправления*.xls",
    "ТО отправления*.csv",
]

OUTPUT_FILE_BASE_NAME = "result_job1"
SOURCE_COLUMN_NAME = "source_file"

INPUT_SHEET_NAME = 0
OUTPUT_SHEET_RAW_NAME = "лист1"
OUTPUT_SHEET_STEP1_NAME = "лист2"
OUTPUT_SHEET_STEP2_NAME = "лист3"
OUTPUT_SHEET_RESULT_NAME = "лист4"

COLUMN_DEST_COUNTRY_NAME = "Страна назначения - наименование"
COLUMN_VEHICLE_NUMBER = "Номер ТС"
COLUMN_PRODUCT_CODE = "Код товара"

EXCLUDED_DEST_COUNTRY = "Республика Казахстан"

ALLOWED_PRODUCT_CODES = {
    "220300", "2204", "2205", "220600", "2207", "2208",
    "2401", "2402", "2403", "4303",
    "6101", "6102", "6103", "6104", "6105", "6106", "6110",
    "6401", "6402", "6403", "6404", "6405",
    "8517", "8519", "8521", "8525", "8526", "8527", "8528",
}


def build_output_file_name(base_name: str) -> str:
    current_date = datetime.now().strftime("%Y%m%d")
    return f"{base_name}_{current_date}.xlsx"


def validate_input_columns(df: pd.DataFrame) -> None:
    required_columns = {
        COLUMN_DEST_COUNTRY_NAME,
        COLUMN_VEHICLE_NUMBER,
        COLUMN_PRODUCT_CODE,
    }
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise KeyError(
            f"В исходной таблице отсутствуют обязательные колонки: {missing}"
        )


def prepare_dataframe(df: pd.DataFrame, source_file_name: str) -> pd.DataFrame:
    result = df.copy()
    result = basic_cleanup(result)
    result = protect_from_excel_formulas(result)

    validate_input_columns(result)

    result[COLUMN_DEST_COUNTRY_NAME] = (
        result[COLUMN_DEST_COUNTRY_NAME].astype(str).str.strip()
    )
    result[COLUMN_VEHICLE_NUMBER] = (
        result[COLUMN_VEHICLE_NUMBER].astype(str).str.strip()
    )
    result[COLUMN_PRODUCT_CODE] = (
        result[COLUMN_PRODUCT_CODE].astype(str).str.strip()
    )

    result[SOURCE_COLUMN_NAME] = source_file_name

    return result


def step_1_remove_kazakhstan(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    rows_before = len(result)

    result = result[
        result[COLUMN_DEST_COUNTRY_NAME] != EXCLUDED_DEST_COUNTRY
    ].copy()

    logger.info(
        f"Шаг 1. Удалены строки по стране назначения "
        f"'{EXCLUDED_DEST_COUNTRY}': {rows_before - len(result)}"
    )
    logger.info(f"Шаг 1. Осталось строк: {len(result)}")

    return result


def step_2_remove_duplicate_vehicle_numbers(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    rows_before = len(result)

    result = result.drop_duplicates(
        subset=[COLUMN_VEHICLE_NUMBER],
        keep="first",
    ).copy()

    logger.info(
        f"Шаг 2. Удалены дубликаты по колонке '{COLUMN_VEHICLE_NUMBER}': "
        f"{rows_before - len(result)}"
    )
    logger.info(f"Шаг 2. Осталось строк: {len(result)}")

    return result


def step_3_filter_product_codes(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    rows_before = len(result)

    def matches_allowed_code(code) -> bool:
        if pd.isna(code):
            return False

        code = str(code).strip()

        for pattern in ALLOWED_PRODUCT_CODES:
            if code.startswith(pattern):
                return True

        return False

    result = result[
        result[COLUMN_PRODUCT_CODE].apply(matches_allowed_code)
    ].copy()

    logger.info(
        f"Шаг 3. Удалены строки вне перечня кодов товара: "
        f"{rows_before - len(result)}"
    )
    logger.info(f"Шаг 3. Осталось строк: {len(result)}")

    return result


def run():
    input_file = get_single_input_file(INPUT_PATTERNS)
    output_file_name = build_output_file_name(OUTPUT_FILE_BASE_NAME)
    output_file = OUT_DIR / output_file_name

    logger.info(f"Найден входной файл: {input_file.name}")

    if input_file.suffix.lower() == ".csv":
        df_raw = read_table(input_file)
    else:
        df_raw = read_excel(input_file, sheet_name=INPUT_SHEET_NAME)

    logger.info(
        f"Лист 1 прочитан: строк={len(df_raw)}, "
        f"колонок={len(df_raw.columns)}"
    )

    prepared_df = prepare_dataframe(df_raw, input_file.name)
    logger.info(f"После подготовки данных строк: {len(prepared_df)}")

    step1_df = step_1_remove_kazakhstan(prepared_df)
    step2_df = step_2_remove_duplicate_vehicle_numbers(step1_df)
    result_df = step_3_filter_product_codes(step2_df)

    sheets_to_write = {
        OUTPUT_SHEET_RAW_NAME: prepared_df,
        OUTPUT_SHEET_STEP1_NAME: step1_df,
        OUTPUT_SHEET_STEP2_NAME: step2_df,
        OUTPUT_SHEET_RESULT_NAME: result_df,
    }

    write_excel_sheets(
        sheets=sheets_to_write,
        file_path=output_file,
        index=False,
    )

    logger.info(f"Результат записан: {output_file}")