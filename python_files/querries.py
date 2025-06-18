from neo4j import GraphDatabase

# conexao
NEO4J_URI = "bolt://slaa"
NEO4J_USER = "slaaa"
NEO4J_PASSWORD = "12345678"

# querries
QUERRIES = {

    #QUERRIE 1
    # Quais cursos estão associados a áreas e setores mais empregados em cada município?
    #  Identifica regiões e setores promissores para alunos de determinados cursos.
    "1. Cursos, áreas e empregos por setor em 2023": """
        MATCH (c:Curso)-[:PERTENCE_A]->(a:AreaAtuacao)
              -[:ESTA_RELACIONADO_A]->(s:SetorEconomico)
              -[r:NUMERO_PESSOAS_EMPREGADAS {ano: 2023}]->(m:Municipio)
        RETURN c.nome AS Curso, a.nome AS Area, s.nome AS Setor, m.nome AS Municipio, r.num_empregados AS Empregados
        ORDER BY Empregados DESC
        LIMIT 20
    """,


# QUERRIE 2
# Quais cursos estão sendo oferecidos por quais instituições, e qual é a média salarial no estado onde a instituição está localizada?
# pode ajudar estudantes a escolher instituições em regiões com melhores perspectivas salariais.


    "2. Cursos, instituições e remuneração média por estado em 2023": """
        MATCH (c:Curso)-[:PERTENCE_A]->(i:InstituicaoSuperior)-[:LOCALIZADA_EM]->(u:UnidadeFederativa)
              -[r:MEDIA_REMUNERACAO_ANUAL {ano: 2023}]->(med:MediaRemuneracao)
        RETURN c.nome AS Curso, i.nome AS Instituicao, u.nome AS Estado, med.media_remuneracao AS Remuneracao
        ORDER BY Remuneracao DESC
        LIMIT 20
    """,
# QUERRIE 3
#Em quais áreas de formação os setores empregam mais pessoas em 2023? E onde?
# Relaciona formação acadêmica, por area de atuacao, com a demanda do mercado local.

    "3. Área, depois setor, depois município com mais empregos em 2023": """
        MATCH (a:AreaAtuacao)-[:ESTA_RELACIONADO_A]->(s:SetorEconomico)
              -[r:NUMERO_PESSOAS_EMPREGADAS {ano: 2023}]->(m:Municipio)
        RETURN a.nome AS Area, s.nome AS Setor, m.nome AS Municipio, r.num_empregados AS Empregados
        ORDER BY Empregados DESC
        LIMIT 20
    """,

# QUERRIE 4
# Quais cursos estão ligados a setores com maior remuneração média por estado em 2023?
# Indica quais formações estão associadas a melhores salários regionais.
    "4. Curso, depois área, depois setor, depois UF com maior remuneração em 2023": """
        MATCH (c:Curso)-[:PERTENCE_A]->(a:AreaAtuacao)-[:ESTA_RELACIONADO_A]->(s:SetorEconomico),
              (u:UnidadeFederativa)-[:MEDIA_REMUNERACAO_ANUAL {ano: 2023}]->(med:MediaRemuneracao)
        RETURN DISTINCT c.nome AS Curso, a.nome AS Area, s.nome AS Setor, u.nome AS Estado, med.media_remuneracao AS Remuneracao
        ORDER BY Remuneracao DESC
        LIMIT 20
    """,
# QUERRIE 5
#"Quais cursos têm mais evasão nas instituições e de quais estados?"
# Ajuda a identificar cursos críticos com alta evasão.

    "5. Cursos com alta evasão (taxa de desistência > 0.5)": """
        MATCH (c:Curso)-[:PERTENCE_A]->(i:InstituicaoSuperior)-[:LOCALIZADA_EM]->(u:UnidadeFederativa),
              (c)-[r:TRAJETORIA_DO_CURSO]->(m:Municipio)
        WHERE toFloat(r.taxa_desistencia) > 0.5
        RETURN DISTINCT c.nome AS Curso, i.nome AS Instituicao, u.nome AS Estado, r.ano AS Ano, r.taxa_desistencia AS Taxa
        ORDER BY Taxa DESC
        LIMIT 20
    """,

# QUERRIE 6
# Quais municípios mais empregaram em setores relacionados à Ciencia da Computacao em 2023?
# Aponta cidades onde há maior demanda prática para cientistas da computacao.
    "6. Top 10 municípios com mais empregos em setores de Ciencia da Computacao em 2023": """
        MATCH (c:Curso {nome: "Ciência da Computação"})-[:PERTENCE_A]->(a:AreaAtuacao)
      -[:ESTA_RELACIONADO_A]->(s:SetorEconomico)
      -[r:NUMERO_PESSOAS_EMPREGADAS {ano: 2023}]->(m:Municipio)
RETURN m.nome AS Municipio, s.nome AS Setor, r.num_empregados AS Empregados
ORDER BY Empregados DESC
LIMIT 10
    """,


# QUERRIE 7
# Investiga se há relação entre o salário médio no estado e o ingresso de alunos em cursos relacionados àquela área de atuação.
# Cursos com maiores salários no estado atraem mais ingressantes naquele ano?

    "7. Ano de entrada em cursos e a pretenção salarial do curso em uma UF": """
        MATCH (c:Curso)-[:PERTENCE_A]->(i:InstituicaoSuperior)-[:LOCALIZADA_EM]->(uf:UnidadeFederativa),
        (c)-[t:TRAJETORIA_DO_CURSO]->(m:Municipio),
        (uf)-[r:MEDIA_REMUNERACAO_ANUAL]->(rem:MediaRemuneracao)
    WHERE t.ano = r.ano
    RETURN c.nome AS Curso, t.ano AS AnoIngresso, uf.nome AS Estado, t.ingressantes AS NumIngressantes, rem.media_remuneracao AS MediaSalarial
    ORDER BY Curso, AnoIngresso
    """,

}

# Função para executar as queries
def run_QUERRIES(driver):
    with driver.session() as session:
        for title, query in QUERRIES.items():
            print(f"\n🔎 {title}\n{'-' * len(title)}")
            result = session.run(query)
            for record in result:
                print(dict(record))

# Função principal
def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        run_QUERRIES(driver)
    finally:
        driver.close()

if __name__ == "__main__":
    main()
