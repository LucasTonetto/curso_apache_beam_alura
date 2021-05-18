import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

# O apache beam faz a leitura linha a linha do arquivo e aplica as transformações nela
# Isso facilita a paralelização do processo
# Tem a opção de batch ou streaming

pipeline_options = PipelineOptions(argv=None)

# Criando uma pipeline
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = [
    "id", 
    "data_iniSE", 
    "casos", 
    "ibge_code", 
    "cidade", 
    "uf", 
    "cep", 
    "latitude", 
    "longitude"
]

def str_to_list(str, delimiter=","):
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos
    """
    return str.split(delimiter)

def list_to_dict(list_to_transform, columns):
    """
    Recebe uma lista de valores e uma lista de chaves
    Retorna um dict { chave: valor }
    """
    return dict(zip(columns, list_to_transform))

def add_year_month(data):
    """
    Recebe um dicionario e cria um campo ANO-MES
    Retorna o dicionario com o novo campo
    """
    data["ano_mes"] = "-".join(data["data_iniSE"].split("-")[:2])
    return data

def uf_key(data):
    """
    Recebe um dicionário
    Retorna uma tupla (UF, data)
    """
    return (data["uf"], data)

def dengue_cases(data):
    """
    Recebe uma tupla ("RS", [{}, {}])
    Retorna uma tupla ("RS-2014-02", 8.0)
    """
    uf, registros = data
    for registro in registros:
        if bool(re.search(r"\d", registro["casos"])):
            # Permite o retorno de vários elementos
            yield (f"{uf}-{registro['ano_mes']}", float(registro["casos"]))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)

def key_uf_year_month(array):
    """
    Recebe uma lista de elementos
    Retorna (UF-YEAR-MONTH, chuva)
    """
    date, mm_chuva, uf = array
    mm = float(mm_chuva)
    if mm < 0.0:
        mm = 0.0
    return f"{uf}-{date[:-3]}", mm

def roud_float(tupla):
    """
    Recebe uma tupla
    Retorna uma tupla com valor arredondado
    """
    key, value = tupla
    return key, round(value, 1)

def filter_empty_fields(elm):
    """
    Remove elementos com chaves vazias
    """
    key, data = elm
    #all > and
    if all([data['chuvas'], data['dengue']]):
        return True
    return False

def unzip(elm):
    """
    Recebe uma tupla
    Retorna uma tupla com a chave e elementos descompactados
    """
    key, data = elm
    chuva = data['chuvas']
    dengue = data['dengue']
    uf, year, month = key.split('-')
    return uf, year, month, str(chuva[0]), str(dengue[0])

def csv_prepare(elm, delimiter=','):
    """
    Recebe uma tupla 
    Retorna uma string delimitada
    """
    return delimiter.join(elm)

# PCollection > recebe o resultado da pipeline
dengue = (
    # Passa a pipeline a ser executada
    # Titulo >> Operação
    pipeline
    | "Leitura do dataset de dengue" 
    # Paraleriza a leitura do arquivo
        >> ReadFromText("./database/sample_casos_dengue.txt", skip_header_lines=1)
        # >> ReadFromText("./database/casos_dengue.txt", skip_header_lines=1)
    | "Transforma um texto para uma lista"
        >> beam.Map(str_to_list, delimiter="|")
        # beam.Map(lambda x: x.split("|"))
    | "Transforma uma lista em um dicionário"
        >> beam.Map(list_to_dict, colunas_dengue)
    | "Adiciona o campo ano_mes ao dicionário"
        >> beam.Map(add_year_month)
    | "Criar chave pelo estado"
        >> beam.Map(uf_key)
    | "Agrupar por chave(estado)"
        # Retorna uma tupla com a chave e um array dos resultados agrupados
        # ("SP", [{...},...])
        >> beam.GroupByKey()
    | "Descompacta casos de dengue"
        # FlatMap para o yield
        >> beam.FlatMap(dengue_cases)
    | "Soma dos casos pela chave"
        >> beam.CombinePerKey(sum)
    # | "Mostrar resultados"
    #     >> beam.Map(print)
)

chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" 
        >> ReadFromText("./database/sample_chuvas.csv", skip_header_lines=1)
        # >> ReadFromText("./database/chuvas.csv", skip_header_lines=1)
    # Não é possível ter duas etapas de pipeline com o mesmo nome
    | "Transforma um texto para uma lista (chuvas)"
        >> beam.Map(str_to_list)
    | "Cria uma chave UF-ANO-MES"
        >> beam.Map(key_uf_year_month)
    | "Soma do total de chuvas pela chave"
        >> beam.CombinePerKey(sum)
    | "Arredonda para uma casa decimal o valor das chuvas"
        >> beam.Map(roud_float)
    # | "Mostrar resultados (chuvas)"
    #     >> beam.Map(print)
)

resultado = (
    # (chuvas, dengue)
    # | "Une as duas PCollections"
    #     >> beam.Flatten()
    # | "Agrupamento das PCollections pela chave"
    #     >> beam.GroupByKey()
    # Informando as PCollections como dicionario (alternativa)
    ({ "chuvas": chuvas, "dengue": dengue })
    | "Mesclar as PCollections"
        >> beam.CoGroupByKey()
    | "Filtrar dados vazios"
        >> beam.Filter(filter_empty_fields)
    | "Descompactar elementos"
        >> beam.Map(unzip)
    | "Preparar o csv"
        >> beam.Map(csv_prepare)
    # | "Mostrar resultado da união"
    #     >> beam.Map(print)
)

header = 'UF,ANO,MES,CHUVA,DENGUE'

resultado | "Criar arquivo csv"  >> WriteToText(
        "./database/resultado", 
        file_name_suffix=".csv",
        header=header
    )

# Executando a pipeline
pipeline.run()

# Leitura > https://cursos.alura.com.br/course/apache-beam-data-pipeline-python/task/92410