from pathlib import Path


# Корень проекта: .../project
BASE_DIR = Path(__file__).resolve().parent.parent

# Каталоги
IN_DIR = BASE_DIR / "in"
OUT_DIR = BASE_DIR / "out"
DATA_DIR = BASE_DIR / "data"

# Справочники локаций для будущей фильтрации
LOCATIONS_KZ_PATH = DATA_DIR / "locations_KZ.json"
LOCATIONS_RB_PATH = DATA_DIR / "locations_RB.json"
LOCATIONS_RF_PATH = DATA_DIR / "locations_RF.json"

LOCATIONS_PATHS = {
    "KZ": LOCATIONS_KZ_PATH,
    "RB": LOCATIONS_RB_PATH,
    "RF": LOCATIONS_RF_PATH,
}

# Создаём выходной каталог, если его нет
OUT_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)


# Общие настройки Excel
DEFAULT_READ_ENGINE = "openpyxl"
DEFAULT_WRITE_ENGINE = "openpyxl"
DEFAULT_OUTPUT_SHEET = "result"


# Имя job по умолчанию
DEFAULT_JOB = "job_example"


# Реестр доступных обработок
# Ключ — имя, которое можно передать в main.py
# Значение — путь до модуля с функцией run()
JOB_MODULES = {
    "job_example": {
        "module": "src.jobs.job_example",
        "description": "Тестовая обработка (пример)",
    },
    "job_seals_structure": {
        "module": "src.jobs.job_seals_structure",
        "description": "Обработка структуры БД печатей",
    },
    "job1": {
        "module": "src.jobs.job1",
        "description": "Удаление Казахстана, дублей по Номер ТС и фильтр по Код товара",
    },
    "job2": {
        "module": "src.jobs.job2",
        "description": "Распределение использованных НП по владельцам, февраль-апрель",
    },
    "job3": {
        "module": "src.jobs.job3",
        "description": "Распределение использованных НП по владельцам, февраль-апрель, с другим источником",
    },
    "jobRF_IN_OUT": {
        "module": "src.jobs.jobRF_IN_OUT",
        "description": "Потоки РФ ↔ КЗ (въезд/выезд + фильтры ТП и владельцев)",
    },
    "1Stage": {
        "module": "src.jobs.1Stage",
        "description": "Фильтровка по 1 этапу ТН ВЭД КОДы)",
    },
    "12ATStageTest": {
        "module": "src.jobs.12ATStageTest",
        "description": "Фильтровка по 1 этапу ТН ВЭД КОДы)",
    },
    "jobPROVERCA_NP": {
        "module": "src.jobs.jobPROVERCA_NP",
        "description": "Сравнение НП номеров и Выгрузки)",
    },
}

