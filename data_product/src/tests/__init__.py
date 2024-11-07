from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent / '../../.env')

INPUT_DELTA_TABLES_PATH = Path(__file__).with_name('resources').resolve()
OUTPUT_DELTA_TABLES_PATH = Path(__file__).with_name('results').resolve()
