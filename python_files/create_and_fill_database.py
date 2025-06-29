import neo4j
from neo4j import GraphDatabase
import argparse

def create_and_fill_database(driver):
    """ Create the Neo4j database and schema.
    Args:
        driver: Neo4j driver to connect to the database.
    """
    
    commands = ["""
    LOAD CSV WITH HEADERS FROM 'file:///indicadores_educacao.csv' AS edu
    FIELDTERMINATOR ';'

    WITH edu, toInteger(edu.area_cod) AS area_cod, edu.nome_area_atuacao AS nome
    MERGE (area:AreaAtuacao {codigo: area_cod})
        ON CREATE SET area.nome=nome

    WITH edu, toInteger(edu.curso_cod) AS curso_cod, edu.curso_nome AS nome, toInteger(edu.grau_academico) AS grau, toInteger(edu.modo_ensino) AS modo
    MERGE (curso:Curso{codigo: curso_cod})
    ON CREATE SET curso.nome=nome, curso.grau_academico=grau, curso.modo_ensino=modo

    WITH edu, toInteger(edu.curso_cod) AS curso_cod, toInteger(edu.area_cod) AS area_cod
    MATCH (c: Curso {codigo: toInteger(edu.curso_cod)})
    MATCH (a:AreaAtuacao {codigo: area_cod})
    MERGE (c)-[:PERTENCE_A]->(a)

    WITH edu, toInteger(edu.inst_cod) AS inst_cod, edu.inst_nome AS nome, edu.categoria_adm AS categoria, edu.org_academica AS org
    MERGE (inst:InstituicaoSuperior {codigo: inst_cod})
    ON CREATE SET inst.nome=nome, inst.categoria_adm=categoria, inst.org_academica=org

    WITH edu, edu.uf_sigla AS uf, edu.uf_nome AS nome
    MERGE (unid:UnidadeFederativa {sigla: uf})
    ON CREATE SET unid.nome=nome 

    WITH edu
    MATCH (c:Curso {codigo: toInteger(edu.curso_cod)})
    MATCH (i:InstituicaoSuperior {codigo: toInteger(edu.inst_cod)})
    MERGE (c)-[:PERTENCE_A]->(i)

    WITH edu
    MATCH (i:InstituicaoSuperior {codigo: toInteger(edu.inst_cod)})
    MATCH (u:UnidadeFederativa {sigla: edu.uf_sigla})
    MERGE (i)-[:LOCALIZADA_EM]->(u)
    """,

    """
    LOAD CSV WITH HEADERS FROM 'file:///rais_tabela4_joined.csv' AS rais4
    FIELDTERMINATOR ';'

    WITH rais4, toInteger(rais4.municipio_cod) AS municipio_cod, rais4.municipio_nome AS nome, rais4.uf_sigla AS uf
    MERGE (mun:Municipio {codigo: municipio_cod})
    ON CREATE SET mun.nome=nome
    WITH*
    MATCH (u:UnidadeFederativa {sigla: uf})
    MERGE (mun)-[:LOCALIZADO_EM]->(u)

    WITH rais4, rais4.setor_nome AS setor
    MERGE (:SetorEconomico {nome: setor})
    """,

    """
    LOAD CSV WITH HEADERS FROM 'file:///indicadores_educacao.csv' AS edu
    FIELDTERMINATOR ';'

    WITH edu, toInteger(edu.municipio_cod) AS municipio_cod, toInteger(edu.ano_referencia) AS ano, edu.num_ingressantes AS ingressantes, edu.num_concluintes AS concluintes, edu.taxa_desistencia AS taxa_desistencia
    MATCH (mun:Municipio {codigo: municipio_cod})
    MATCH (c:Curso {codigo: toInteger(edu.curso_cod)})
    MERGE (c)-[:TRAJETORIA_DO_CURSO {ano: ano, ingressantes: ingressantes, concluintes: concluintes,taxa_desistencia: taxa_desistencia}]->(mun)
    """,

    """
    LOAD CSV WITH HEADERS FROM 'file:///rais_tabela6_joined.csv' AS rais6
    FIELDTERMINATOR ';'

    WITH rais6, rais6.uf_sigla AS uf, toFloat(rais6.media_remuneracao) AS media_remuneracao, toInteger(rais6.ano) AS ano
    MERGE (med:MediaRemuneracao {media_remuneracao: media_remuneracao})

    WITH *
    MATCH (u:UnidadeFederativa {sigla: uf})
    MATCH (med:MediaRemuneracao {media_remuneracao: media_remuneracao})
    MERGE (u)-[:MEDIA_REMUNERACAO_ANUAL {ano: ano}]->(med)
    """,

    """
    LOAD CSV WITH HEADERS FROM 'file:///rais_tabela4_joined.csv' AS rais4
    FIELDTERMINATOR ';'

    WITH rais4, toInteger(rais4.municipio_cod) AS municipio_cod, toInteger(rais4.num_pessoas_empregadas) AS num_empregados, rais4.setor_nome AS setor_nome, toInteger(rais4.ano) AS ano
    MATCH (mun:Municipio {codigo: municipio_cod})
    MATCH (setor:SetorEconomico {nome: setor_nome})
    MERGE (setor)-[:NUMERO_PESSOAS_EMPREGADAS {num_empregados: num_empregados, ano:ano}]->(mun)
    """,
    
    """
    MATCH (a:AreaAtuacao {codigo: 1}), 
    (s:SetorEconomico {nome: 'Serviços'})
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s)
    """,

    """
    MATCH (a:AreaAtuacao {codigo: 2}), 
    (s:SetorEconomico {nome: 'Serviços'})
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s)
    """,

    """
    MATCH (a:AreaAtuacao {codigo: 3}),
    (s:SetorEconomico {nome: 'Serviços'})
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s)
    """,

    """
    MATCH (a:AreaAtuacao {codigo: 4}),
    (s1:SetorEconomico {nome: 'Serviços'}),
    (s2:SetorEconomico {nome: 'Comércio'})
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s1)
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s2)
    """,

    """
    MATCH (a:AreaAtuacao {codigo: 5}),
    (s1:SetorEconomico {nome: 'Serviços'}),
    (s2:SetorEconomico {nome: 'Comércio'}),
    (s3:SetorEconomico {nome: 'Indústria'})
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s1)
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s2)
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s3)
    """,

    """
    MATCH (a:AreaAtuacao {codigo: 6}),
    (s1:SetorEconomico {nome: 'Serviços'}),
    (s2:SetorEconomico {nome: 'Comércio'}),
    (s3:SetorEconomico {nome: 'Indústria'})
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s1)
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s2)
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s3)
    """,

    """
    MATCH (a:AreaAtuacao {codigo: 7}),
    (s1:SetorEconomico {nome: 'Agropecuária'}),
    (s2:SetorEconomico {nome: 'Indústria'}),
    (s3:SetorEconomico {nome: 'Construção'})
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s1)
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s2)
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s3)
    """,

    """
    MATCH (a:AreaAtuacao {codigo: 8}),
    (s1:SetorEconomico {nome: 'Agropecuária'}),
    (s2:SetorEconomico {nome: 'Indústria'}),
    (s3:SetorEconomico {nome: 'Serviços'})
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s1)
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s2)
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s3)
    """,

    """
    MATCH (a:AreaAtuacao {codigo: 9}),
    (s:SetorEconomico {nome: 'Serviços'})
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s)
    """,

    """
    MATCH (a:AreaAtuacao {codigo: 10}),
    (s1:SetorEconomico {nome: 'Serviços'}),
    (s2:SetorEconomico {nome: 'Comércio'})
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s1)
    MERGE (a)-[:ESTA_RELACIONADO_A]->(s2)
    """
    ]

    for command in commands:
        with driver.session() as session:
            session.run(command)
            print("Command executed successfully.")


def main(create_bool):
    """ Main function to create the Neo4j database and schema.

    Args:
        create_bool: Boolean to create the graph database.
    """

    URL = "bolt://localhost:7687" # Replace with your Neo4j database URL
    auth = ("neo4j", "12345678")  # Replace with your Neo4j username and password
    
    driver = neo4j.GraphDatabase.driver(
        uri = URL, 
        auth = auth
    )
    
    if create_bool == "True":
        create_and_fill_database(driver)
    
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




