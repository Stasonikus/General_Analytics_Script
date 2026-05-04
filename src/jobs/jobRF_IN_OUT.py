from datetime import datetime
import pandas as pd

from src.core.config import OUT_DIR
from src.io.io_excel import get_single_input_file
from src.core.logger import get_logger


logger = get_logger(__name__)


INPUT_PATTERNS = [
    "Количество НП на ТП*.xlsx",
    "количество нп на тп*.xlsx",
]

OUTPUT_FILE_BASE_NAME = "rf_to_kz_tp"


# -------------------------
# 🎯 НУЖНЫЕ Т/П
# -------------------------
TARGET_TP = [
    'НУР ЖОЛЫ',
    'КАЗЫГУРТ',
    'ИМЕНИ БАУЫРЖАНА КОНЫСБАЕВА',
    'ТЕМИРЖОЛ',
    'ТАЖЕН',
    'АТАМЕКЕН',
]


# -------------------------
# 🇷🇺 РФ признаки
# -------------------------
RU_KEYS = [
    "РОСС",
    "МОСК",
    "ОМСК",
    "ЧЕЛЯБ",
    "ЕКАТЕРИН",
    "НОВОСИБ",
    "ТАМОЖЕННЫЙ ПОСТ",
    "СВХ",
]


# -------------------------
# 🔧 Утилиты
# -------------------------
def norm(x):
    if pd.isna(x):
        return ""
    return str(x).upper().strip()


def is_ru(text):
    t = norm(text)
    return any(k in t for k in RU_KEYS)


def extract_tp(text):
    """
    Ищет Т/П из списка внутри строки
    """
    t = norm(text)

    for tp in TARGET_TP:
        if tp in t:
            return tp

    return None


def normalize_columns(df):
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.replace("\n", " ")
    )
    return df


def find_column(df, keywords):
    for col in df.columns:
        col_low = col.lower()
        for key in keywords:
            if key in col_low:
                logger.info(f"Найдена колонка: {col}")
                return col
    raise KeyError(f"Колонка не найдена: {keywords}")


def build_file():
    return f"{OUTPUT_FILE_BASE_NAME}_{datetime.now().strftime('%Y%m%d')}.xlsx"


# -------------------------
# 🚀 MAIN
# -------------------------
def run():
    try:
        file = get_single_input_file(INPUT_PATTERNS)
        output = OUT_DIR / build_file()

        logger.info(f"Файл: {file.name}")

        # читаем без header
        raw = pd.read_excel(file, header=None)

        # ищем строку заголовков
        header_row = None
        for i, row in raw.iterrows():
            vals = [str(x).lower() for x in row if pd.notna(x)]
            if any("start" in v for v in vals):
                header_row = i
                break

        if header_row is None:
            raise ValueError("❌ Не найден header")

        df = pd.read_excel(file, header=header_row)
        df = normalize_columns(df)

        logger.info(f"Строк: {len(df)}")

        col_start = find_column(df, ["start"])
        col_end = find_column(df, ["end"])

        # нормализация
        df["start_norm"] = df[col_start].apply(norm)
        df["end_norm"] = df[col_end].apply(norm)

        # -------------------------
        # 🎯 ФИЛЬТР РФ → КЗ
        # -------------------------
        df = df[
            df["start_norm"].apply(is_ru)
        ].copy()

        # извлекаем ТП
        df["tp"] = df["end_norm"].apply(extract_tp)

        # оставляем только нужные ТП
        df = df[df["tp"].notna()]

        logger.info(f"После фильтра: {len(df)}")

        # -------------------------
        # 📊 ГРУППИРОВКА
        # -------------------------
        result = (
            df.groupby("tp")
            .size()
            .reset_index(name="Количество перевозок")
            .sort_values(by="Количество перевозок", ascending=False)
        )

        total = result["Количество перевозок"].sum()

        result.loc[len(result)] = ["ИТОГО", total]

        logger.info(f"ИТОГО: {total}")

        # -------------------------
        # 💾 ВЫГРУЗКА
        # -------------------------
        with pd.ExcelWriter(output) as writer:
            result.to_excel(writer, sheet_name="RF_to_KZ", index=False)

        logger.info(f"Готово: {output}")

    except Exception as e:
        logger.exception(f"❌ Ошибка: {e}")
        raise