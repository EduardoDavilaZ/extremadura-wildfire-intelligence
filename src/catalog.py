# Rutas de Volumes y tablas Unity Catalog (workspace.default.*)

CATALOG = "workspace"
SCHEMA = "default"
UC = f"{CATALOG}.{SCHEMA}"

BRONZE_AEMET_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/bronze_aemet/data-2015-2026.jsonl"
BRONZE_FIRMS_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/bronze_nasa_firms/data-2015-2026.json"
BRONZE_MITECO_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/bronze_miteco/data-2015-2026.xml"

SILVER_AEMET_TABLE = f"{UC}.silver_aemet_diarios_extremadura"
SILVER_FIRMS_TABLE = f"{UC}.silver_nasa_firms"
SILVER_MITECO_TABLE = f"{UC}.silver_miteco_incendios"

GOLD_MITECO_ANNUAL_TABLE = f"{UC}.gold_miteco_resumen_anual"
GOLD_FIRMS_MONTHLY_TABLE = f"{UC}.gold_firms_monthly"
GOLD_FIRMS_GRID_TABLE = f"{UC}.gold_firms_grid"
GOLD_FIRMS_PREV_7D_TABLE = f"{UC}.gold_firms_prev_7d"

EXTREMADURA_PROVINCE_CODES = ("06", "10")
EXTREMADURA_LAT_MIN = 37.94
EXTREMADURA_LAT_MAX = 40.49
EXTREMADURA_LON_MIN = -7.55
EXTREMADURA_LON_MAX = -4.65
FIRMS_BBOX = f"{EXTREMADURA_LON_MIN},{EXTREMADURA_LAT_MIN},{EXTREMADURA_LON_MAX},{EXTREMADURA_LAT_MAX}"
FIRMS_GRID_STEP = 0.05
