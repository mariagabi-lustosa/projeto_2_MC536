import neo4j
import argparse

def create_and_fill_database(driver):
    """ Create the Neo4j database and schema.
    Args:
        driver: Neo4j driver to connect to the database.
    """
    
    graph = """

    LOAD CSV WITH HEADERS FROM 'file:///indicadores_educacao.csv' AS row
    FIELDTERMINATOR ';'
    WITH DISTINCT toInteger(row.area_cod) AS area_cod, row.area_atuacao AS nome
    MERGE (:AreaAtuacao {codigo: area_cod, nome: nome})
    WITH DISTINCT toInteger(row.curso_cod) AS curso_cod, row.curso_nome AS nome, toInteger(row.grau_academico) AS grau, toInteger(row.modo_ensino) AS modo
    MERGE (:Curso{codigo: curso_cod, nome: nome, grau_academico: grau, modo_ensino: modo})
    WITH DISTINCT toInteger(row.inst_cod) AS inst_cod, row.inst_nome AS nome, row.categoria_adm AS categoria, row.org_academica AS org
    MERGE (:InstituicaoSuperior {codigo: inst_cod, nome: nome, categoria_adm: categoria, org_academica: org})
    WITH DISTINCT row.uf_sigla AS uf, row.uf_nome AS nome
    MERGE (:UnidadeFederativa {sigla: uf, nome: nome})
    MATCH (c:Crso {codigo: toInteger(row.curso_cod)})
    MATCH (i:InstituicaoSuperior {codigo: toInteger(row.inst_cod)})
    MERGE (c)-[:PERTENCE_A]->(i)
    

    LOAD CSV WITH HEADERS FROM 'file:///rais_tabela4_joined.csv' AS row
    FIELDTERMINATOR ';'
    WITH DISTINCT toInteger(row.municipio_cod) AS municipio_cod, row.municipio_nome AS nome
    MERGE (:Municipio {codigo: municipio_cod, nome: nome})
    WITH DISTINCT row.setor_nome AS setor
    MERGE (:Setor {nome: setor})

    LOAD CSV WITH HEADERS FROM 'file:///rais_tabela6_joined.csv' AS row



def main(create_bool):
    """ Main function to create the Neo4j database and schema.

    Args:
        create_bool: Boolean to create the database and tables.
    """

    URL = "###" # Replace with your Neo4j database URL
    auth = ("neo4j", "password")  # Replace with your Neo4j username and password
    
    driver = neo4j.GraphDatabase.driver(
        uri = URL, 
        auth = auth
    )
    
    if create_bool == "True":
        create_database(driver)
    
    driver.close()
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a Neo4j database with the specified schema."
    )
    parser.add_argument(
        "--create",
        "-c",
        default=False,
        help="Create the Neo4j database and tables."
    )
    args = parser.parse_args()
    create_bool = args.create
    main(create_bool)




