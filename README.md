# Apache Beam - Básico
## Projeto
Esse projeto foi feito com base no curso [Apache Beam: Data Pipeline com Python](https://cursos.alura.com.br/course/apache-beam-data-pipeline-python) da alura.
O projeto consiste na criação de uma pipeline, com o Apache Beam, que tem como finalidade o processo de ETL nas bases de dados de incidência de chuva em cada estado e casos de dengue em cada estado.
Os dados são lidos das bases presentes na pasta **database**, agrupados por mês/ano para cada estado e a saída é salva em um arquivo csv dentro da mesma pasta.

### Databases
As bases de dados sem amostragem podem ser encontradas [neste link](https://caelum-online-public.s3.amazonaws.com/1954-apachebeam/alura-apachebeam-basedados.rar).