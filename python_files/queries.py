from neo4j import GraphDatabase
import csv
import os

# Conexão
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"

# Queries
QUERIES = {

    # QUERY 1
    # No setor de agrupecuária, quais instituições oferecem cursos em áreas de atuação relacionadas a ele e qual o número de pessoas empregadas neste setor em cada município?
    # Identifica regiões com maior empregabilidade para alunos de determinada área.

    "1. Cursos, áreas e empregos por setor em 2023": """
        MATCH (s:SetorEconomico)
        WHERE toLower(s.nome) CONTAINS "agropecuária" 
        MATCH (c:Curso)
        WHERE toLower(c.nome) CONTAINS "agro" OR toLower(c.nome) CONTAINS "amb"
        MATCH (c:Curso)-[:PERTENCE_A]->(a:AreaAtuacao)-[:ESTA_RELACIONADO_A]->(s),
        (s)-[r:NUMERO_PESSOAS_EMPREGADAS {ano: 2023}]->(m:Municipio)
        RETURN DISTINCT c.nome AS Curso, a.nome AS Area, s.nome AS Setor, m.nome AS Municipio, r.num_empregados AS Empregados
        ORDER BY Empregados DESC
    """,

    # QUERY 2
    # Procurando por um curso específico, buscamos quais são as instituições que oferecem-no e qual a média de remuneração do estado no qual ela está localizada.
    # Pode ajudar estudantes a escolher instituições em regiões com melhores perspectivas salariais, caso esse seja o objetivo. Saber a média salarial do estado como um todo é interessante pois muitas vezes as pessoas são graduadas em uma área e acabam migrando para outra ao se formarem ou ao longo da vida.

    "2. Cursos, instituições e remuneração média por estado em 2023": """
        MATCH (c:Curso)
        WHERE toLower(c.nome) CONTAINS "computação"
        MATCH (c)-[:PERTENCE_A]->(i:InstituicaoSuperior)-[:LOCALIZADA_EM]->(u:UnidadeFederativa),
        (u)-[r:MEDIA_REMUNERACAO_ANUAL {ano: 2023}]->(med:MediaRemuneracao)
        RETURN DISTINCT c.nome AS Curso, i.nome AS Instituicao, u.nome AS Estado, med.media_remuneracao AS Remuneracao
        ORDER BY Curso ASC, Remuneracao DESC
    """,

    # QUERY 3
    # Em quais áreas de atuação os setores empregaram mais pessoas em 2023 no estado de São Paulo, com exceção de sua capital?
    # Relaciona formação acadêmica por área de atuação com a demanda do mercado local.

    "3. Área, depois setor, depois município com mais empregos em 2023": """
        MATCH (uf:UnidadeFederativa)
        WHERE uf.sigla CONTAINS 'SP'
        MATCH (m:Municipio)
        WHERE m.nome <> 'São Paulo'
        MATCH (a:AreaAtuacao)-[:ESTA_RELACIONADO_A]->(s:SetorEconomico),
        (s)-[r:NUMERO_PESSOAS_EMPREGADAS {ano: 2023}]->(m), (m)-[:LOCALIZADO_EM]->(uf) 
        RETURN a.nome AS Area, s.nome AS Setor, m.nome AS Municipio, r.num_empregados AS Empregados
        ORDER BY Empregados DESC
    """,

    # QUERY 4
    # Quais cursos têm mais evasão nas instituições e de quais estados?
    # Ajuda a identificar cursos críticos com alta evasão.

    "4. Cursos com alta evasão (Taxa de desistência > 50.0)": """
        MATCH (c:Curso)-[:PERTENCE_A]->(i:InstituicaoSuperior)-[:LOCALIZADA_EM]->(u:UnidadeFederativa),
              (c)-[r:TRAJETORIA_DO_CURSO]->(m:Municipio)
        WHERE toFloat(r.taxa_desistencia) > 0.5
        RETURN DISTINCT c.nome AS Curso, i.nome AS Instituicao, u.nome AS Estado, r.ano AS Ano, r.taxa_desistencia AS Taxa
        ORDER BY Taxa DESC
        LIMIT 1000
    """,

    # QUERY 5
    # Cursos com maiores salários no estado atraem mais ingressantes naquele ano?
    # Investiga se há relação entre o salário médio no estado e o ingresso de alunos em cursos relacionados àquela área de atuação.

    "5. Ano de entrada em cursos e a pretenção salarial do curso em uma UF": """
        MATCH (c:Curso)-[:PERTENCE_A]->(i:InstituicaoSuperior)-[:LOCALIZADA_EM]->(uf:UnidadeFederativa), (c)-[t:TRAJETORIA_DO_CURSO {ano: 2022}]->(:Municipio), (uf)-[r:MEDIA_REMUNERACAO_ANUAL {ano: 2022}]->(rem:MediaRemuneracao)
        WITH 
            c.nome AS Curso,
            uf.nome AS Estado,
            sum(toInteger(t.ingressantes)) AS NumIngressantes,
            avg(toFloat(rem.media_remuneracao)) AS MediaSalarial
        RETURN 
            Curso,
            Estado,
            NumIngressantes,
            round(MediaSalarial, 2) AS MediaSalarial
        ORDER BY Estado, NumIngressantes DESC;
    """,

    # QUERY 6
    # Qual a relação entre estados que tiveram queda na remuneração média e o aumento da taxa de desistência dos cursos de graduação?
    # Ajuda a entender se a diminuição da remuneração média está correlacionada com o aumento da taxa de desistência dos cursos.

    "6. Relação entre estados com queda na remuneração e taxa de desistência média dos cursos de graduação": """
        MATCH (uf:UnidadeFederativa)
        MATCH (uf)-[:MEDIA_REMUNERACAO_ANUAL {ano: 2020}]->(rem2020:MediaRemuneracao), (uf)-[:MEDIA_REMUNERACAO_ANUAL {ano: 2023}]->(rem2023:MediaRemuneracao)
        
        WITH uf, (toFloat(rem2023.media_remuneracao) - toFloat(rem2020.media_remuneracao)) AS delta_remuneracao

        MATCH (uf)<-[:LOCALIZADA_EM]-(i:InstituicaoSuperior)<-[:PERTENCE_A]-(c:Curso),(c)-[traj2020:TRAJETORIA_DO_CURSO {ano: 2020}]->(:Municipio), (c)-[traj2023:TRAJETORIA_DO_CURSO {ano: 2023}]->(:Municipio)

        WITH 
            uf.nome AS uf_nome,
            delta_remuneracao,
            avg(toFloat(traj2023.taxa_desistencia)) - avg(toFloat(traj2020.taxa_desistencia)) AS delta_desistencia

        WHERE delta_desistencia > 0 AND delta_remuneracao < 0
        RETURN uf_nome AS Estado, round(delta_desistencia, 2) AS Aumento_Desistencia, round(delta_remuneracao, 2) AS Variacao_Remuneracao
        ORDER BY Aumento_Desistencia DESC;
    """
}

# Função para executar as queries e salvar em CSV
def run_queries(driver):
    """Executes a series of predefined queries against a Neo4j database and saves the results to CSV files.

    Args:
        driver (GraphDatabase.Driver): The Neo4j driver to connect to the database.
    """
    os.makedirs("queries", exist_ok=True)  # Create a folder for CSVs

    with driver.session() as session:
        for idx, (title, query) in enumerate(QUERIES.items(), start=1):
            print(f"\n {title}\n{'-' * len(title)}")
            result = session.run(query)

            records = [dict(record) for record in result]

            # Print to terminal
            for record in records:
                print(record)

            # If there are results, write to CSV
            if records:
                filename = f"queries/query_{idx}_result.csv"
                with open(filename, mode="w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=records[0].keys())
                    writer.writeheader()
                    writer.writerows(records)
                print(f"Saved to {filename}")
            else:
                print("No results found.")

# Função principal
def main():
    """Main function to execute a series of Neo4j queries and save results to CSV files.
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        run_queries(driver)
    finally:
        driver.close()

if __name__ == "__main__":
    main()