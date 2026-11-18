import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def parse_miteco_xml_to_df(spark: SparkSession, xml_path: str) -> DataFrame:
    # Lee un archivo XML de partes de incendios del MITECO y devuelve un DataFrame tipado.
    tree = ET.parse(xml_path)
    root = tree.getroot()

    incendios_data = []

    for pif in root.findall('.//{*}Pif'):
        id_pif = pif.findtext('{*}idpif') or pif.findtext('{*}IdPif')
        num_parte = pif.findtext('{*}numeroparte') or pif.findtext('{*}NumeroParte')

        comun = pif.find('{*}pif_comun')
        anio = comun.findtext('{*}anio') if comun is not None else None
        if not num_parte and comun is not None:
            num_parte = comun.findtext('{*}numeroparte')

        loc = pif.find('{*}pif_localizacion')
        provincia = loc.findtext('{*}idprovincia') if loc is not None else None
        municipio = loc.findtext('{*}idmunicipio') if loc is not None else None
        paraje = loc.findtext('{*}paraje') if loc is not None else None
        lat = loc.findtext('{*}latitud') if loc is not None else None
        lon = loc.findtext('{*}longitud') if loc is not None else None

        incendios_data.append((
            int(id_pif) if id_pif and id_pif.isdigit() else None,
            int(num_parte) if num_parte and num_parte.isdigit() else None,
            int(anio) if anio and anio.isdigit() else None,
            str(provincia) if provincia else None,
            str(municipio) if municipio else None,
            str(paraje) if paraje else None,
            str(lat) if lat else None,
            str(lon) if lon else None
        ))

    schema = StructType([
        StructField("id_pif", IntegerType(), True),
        StructField("num_parte", IntegerType(), True),
        StructField("anio", IntegerType(), True),
        StructField("provincia", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("paraje", StringType(), True),
        StructField("latitud", StringType(), True),
        StructField("longitud", StringType(), True)
    ])

    return spark.createDataFrame(incendios_data, schema=schema)