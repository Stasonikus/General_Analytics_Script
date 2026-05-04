import pandas as pd

from src.config import OUT_DIR, ARH_DIR
from src.io_excel import get_single_input_file, write_excel_sheets
from src.job_utils import read_input_file, add_source_file_column
from src.transform import basic_cleanup, protect_from_excel_formulas
from src.common import (
    build_output_file_name,
    validate_required_columns,
    archive_existing_output,
)
from src.logger import get_logger


logger = get_logger(__name__)


# =========================
# НАСТРОЙКИ JOB
# =========================

INPUT_PATTERNS = [
    "test*.xlsx",
    "test*.xls",
    "test*.csv",
]

OUTPUT_FILE_BASE_NAME = "result_job_template"
SOURCE_COLUMN_NAME = "source_file"

INPUT_SHEET_NAME = 0

OUTPUT_SHEET_RAW_NAME = "лист1"
OUTPUT_SHEET_RESULT_NAME = "лист2"

REQUIRED_COLUMNS: set[str] = set()


# =========================
# ПОДГОТОВКА ДАННЫХ
# =========================

def validate_input_columns(df: pd.DataFrame) -> None:
    """
    Проверка обязательных колонок для конкретной job.
    """
    validate_required_columns(df, REQUIRED_COLUMNS)


def normalize_job_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Локальная заглушка нормализации для конкретной job.

    Здесь можно:
    - trim нужных колонок
    - приводить даты через pd.to_datetime(...)
    - приводить коды/номера к строке
    """
    result = df.copy()
    return result


def prepare_dataframe(df: pd.DataFrame, source_file_name: str) -> pd.DataFrame:
    """
    Типовая подготовка:
    - базовая очистка
    - защита от Excel-формул
    - локальная нормализация
    - валидация обязательных колонок
    - добавление source_file
    """
    result = df.copy()

    result = basic_cleanup(result)
    result = protect_from_excel_formulas(result)
    result = normalize_job_dataframe(result)

    validate_input_columns(result)

    result = add_source_file_column(
        df=result,
        source_file_name=source_file_name,
        column_name=SOURCE_COLUMN_NAME,
    )

    return result


# =========================
# ШАГИ ОБРАБОТКИ
# =========================

def step_1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Заглушка шага 1.
    """
    result = df.copy()
    return result


def step_2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Заглушка шага 2.
    """
    result = df.copy()
    return result


def build_result_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Финальная сборка результата.
    """
    result = df.copy()
    return result


def build_output_sheets(
    prepared_df: pd.DataFrame,
    step1_df: pd.DataFrame,
    step2_df: pd.DataFrame,
    result_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """
    Возвращает набор листов для записи.
    При необходимости переопределяется в конкретной job.
    """
    return {
        OUTPUT_SHEET_RAW_NAME: prepared_df,
        OUTPUT_SHEET_RESULT_NAME: result_df,
    }


# =========================
# RUN
# =========================

def run():
    input_file = get_single_input_file(INPUT_PATTERNS)
    output_file_name = build_output_file_name(OUTPUT_FILE_BASE_NAME)
    output_file = OUT_DIR / output_file_name

    logger.info(f"Найден входной файл: {input_file.name}")

    df_raw = read_input_file(
        file_path=input_file,
        sheet_name=INPUT_SHEET_NAME,
    )

    logger.info(
        f"Входной файл прочитан: строк={len(df_raw)}, "
        f"колонок={len(df_raw.columns)}"
    )

    prepared_df = prepare_dataframe(df_raw, input_file.name)
    logger.info(f"После подготовки данных строк: {len(prepared_df)}")

    step1_df = step_1(prepared_df)
    logger.info(f"После шага 1 строк: {len(step1_df)}")

    step2_df = step_2(step1_df)
    logger.info(f"После шага 2 строк: {len(step2_df)}")

    result_df = build_result_dataframe(step2_df)
    logger.info(f"Итоговых строк: {len(result_df)}")

    sheets_to_write = build_output_sheets(
        prepared_df=prepared_df,
        step1_df=step1_df,
        step2_df=step2_df,
        result_df=result_df,
    )

    archive_existing_output(
        output_file=output_file,
        archive_dir=ARH_DIR,
        keep_last=10,
    )

    write_excel_sheets(
        sheets=sheets_to_write,
        file_path=output_file,
        index=False,
    )

    logger.info(f"Результат записан: {output_file}")