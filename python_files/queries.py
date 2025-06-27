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
    # Quais cursos estão associados a áreas e setores mais empregados em cada município?
    # Identifica regiões e setores promissores para alunos de determinados cursos.

    "1. Cursos, áreas e empregos por setor em 2023": """
        MATCH (c:Curso)-[:PERTENCE_A]->(a:AreaAtuacao)-[:ESTA_RELACIONADO_A]->(s:SetorEconomico),
        (s)-[r:NUMERO_PESSOAS_EMPREGADAS {ano: 2023}]->(m:Municipio)
        RETURN DISTINCT c.nome AS Curso, a.nome AS Area, s.nome AS Setor, m.nome AS Municipio, r.num_empregados AS Empregados
        ORDER BY Empregados DESC
        LIMIT 20
    """,

    # QUERY 2
    # Quais cursos estão sendo oferecidos por quais instituições, e qual é a média salarial no estado onde a instituição está localizada?
    # Pode ajudar estudantes a escolher instituições em regiões com melhores perspectivas salariais.

    "2. Cursos, instituições e remuneração média por estado em 2023": """
        MATCH (c:Curso)-[:PERTENCE_A]->(i:InstituicaoSuperior)-[:LOCALIZADA_EM]->(u:UnidadeFederativa),
        (u)-[r:MEDIA_REMUNERACAO_ANUAL {ano: 2023}]->(med:MediaRemuneracao)
        RETURN DISTINCT c.nome AS Curso, i.nome AS Instituicao, u.nome AS Estado, med.media_remuneracao AS Remuneracao
        ORDER BY Remuneracao DESC
        LIMIT 20
    """,

    # QUERY 3
    # Em quais áreas de formação os setores empregam mais pessoas em 2023? E onde?
    # Relaciona formação acadêmica, por area de atuacao, com a demanda do mercado local.

    "3. Área, depois setor, depois município com mais empregos em 2023": """
        MATCH (a:AreaAtuacao)-[:ESTA_RELACIONADO_A]->(s:SetorEconomico),
        (s)-[r:NUMERO_PESSOAS_EMPREGADAS {ano: 2023}]->(m:Municipio)
        RETURN a.nome AS Area, s.nome AS Setor, m.nome AS Municipio, r.num_empregados AS Empregados
        ORDER BY Empregados DESC
        LIMIT 20
    """,

    # QUERY 4
    # Quais cursos estão ligados a setores com maior remuneração média por estado em 2023?
    # Indica quais formações estão associadas a melhores salários regionais.

    "4. Curso, depois área, depois setor, depois UF com maior remuneração em 2023": """
        MATCH (c:Curso)-[:PERTENCE_A]->(a:AreaAtuacao)-[:ESTA_RELACIONADO_A]->(s:SetorEconomico),
        (c)-[:PERTENCE_A]->(i:InstituicaoSuperior)-[:LOCALIZADA_EM]->(u:UnidadeFederativa),
        (u)-[r:MEDIA_REMUNERACAO_ANUAL {ano: 2023}]->(med:MediaRemuneracao)
        RETURN DISTINCT c.nome AS Curso, a.nome AS Area, s.nome AS Setor, u.nome AS Estado, med.media_remuneracao AS Remuneracao
        ORDER BY Remuneracao DESC
        LIMIT 20
    """,

    # QUERY 5
    # Quais cursos têm mais evasão nas instituições e de quais estados?
    # Ajuda a identificar cursos críticos com alta evasão.

    "5. Cursos com alta evasão (Taxa de desistência > 50.0)": """
        MATCH (c:Curso)-[:PERTENCE_A]->(i:InstituicaoSuperior)-[:LOCALIZADA_EM]->(u:UnidadeFederativa),
              (c)-[r:TRAJETORIA_DO_CURSO]->(m:Municipio)
        WHERE toFloat(r.taxa_desistencia) > 0.5
        RETURN DISTINCT c.nome AS Curso, i.nome AS Instituicao, u.nome AS Estado, r.ano AS Ano, r.taxa_desistencia AS Taxa
        ORDER BY Taxa DESC
        LIMIT 20
    """,

    # QUERY 6
    # Quais municípios mais empregaram em setores relacionados à Ciencia da Computacao em 2023?
    # Aponta cidades onde há maior demanda prática para cientistas da computacao.
    
    "6. Top 10 municípios com mais empregos em setores de Ciencia da Computacao em 2023": """
        MATCH (c:Curso)
        WHERE toLower(c.nome) CONTAINS "computação"
        MATCH (c)-[:PERTENCE_A]->(a:AreaAtuacao)-[:ESTA_RELACIONADO_A]->(s:SetorEconomico),
              (s)-[r:NUMERO_PESSOAS_EMPREGADAS {ano:2023}]->(m:Municipio)
        RETURN m.nome AS Municipio, s.nome AS Setor, r.num_empregados AS Empregados
        ORDER BY Empregados DESC
        LIMIT 10
    """,

    # QUERY 7
    # Cursos com maiores salários no estado atraem mais ingressantes naquele ano?
    # Investiga se há relação entre o salário médio no estado e o ingresso de alunos em cursos relacionados àquela área de atuação.

    "7. Ano de entrada em cursos e a pretenção salarial do curso em uma UF": """
        MATCH (c:Curso)-[:PERTENCE_A]->(i:InstituicaoSuperior)-[:LOCALIZADA_EM]->(uf:UnidadeFederativa),
        (c)-[t:TRAJETORIA_DO_CURSO]->(m:Municipio),
        (uf)-[r:MEDIA_REMUNERACAO_ANUAL]->(rem:MediaRemuneracao)
        WHERE t.ano = r.ano
        RETURN c.nome AS Curso, t.ano AS AnoIngresso, uf.nome AS Estado, t.ingressantes AS NumIngressantes, rem.media_remuneracao AS MediaSalarial
        ORDER BY Curso, AnoIngresso
    """


# QUERY do prj 1
        "8. Relação entre estados com queda na remuneração e taxa de desistência média dos cursos de graduação":"""
        WITH MediaRemuneracao AS (
            SELECT 
                UnidadeFederativa,
                MAX(CASE WHEN ano = 2023 THEN media_remuneracao END) -
                MIN(CASE WHEN ano = 2020 THEN media_remuneracao END) AS delta_remuneracao
            FROM "Remuneracao_Media_Por_UF"
            WHERE ano IN (2020, 2023)
            GROUP BY UnidadeFederativa
        ),
        DesistenciaDelta AS (
            SELECT 
                i.UnidadeFederativa,
                AVG(CASE WHEN tc.ano_referencia = 2023 THEN tc.taxa_desistencia END) -
                AVG(CASE WHEN tc.ano_referencia = 2020 THEN tc.taxa_desistencia END) AS delta_desistencia
            FROM "Trajetoria_Curso" tc
            JOIN "Curso" c ON tc.curso_cod = c.curso_cod
            JOIN "Instituicao_Superior" i ON c.inst_cod = i.inst_cod
            WHERE tc.ano_referencia IN (2020, 2023)
            GROUP BY i.UnidadeFederativa
        )
        SELECT 
            uf.uf_nome,
            ROUND(d.delta_desistencia, 2) AS aumento_desistencia,
            ROUND(r.delta_remuneracao, 2) AS variacao_remuneracao
        FROM DesistenciaDelta d
        JOIN MediaRemuneracao r ON d.UnidadeFederativa = r.UnidadeFederativa
        JOIN "Unidade_Federativa" uf ON uf.UnidadeFederativa = d.UnidadeFederativa
        WHERE d.delta_desistencia > 0 AND r.delta_remuneracao < 0
        ORDER BY d.delta_desistencia DESC;
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