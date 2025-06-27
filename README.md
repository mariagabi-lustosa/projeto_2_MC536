# Projeto 2 de MC536 - Análise de Concluintes do Ensino Superior e Mercado de Trabalho via Grafos

## 👥 Integrantes (ID 22):  
Maria Gabriela Lustosa Oliveira - RA: 188504  
Gabriel Cabral Romero Oliveira - RA: 247700  
Flavia Juliana Ventilari dos Santos - RA: 260438     


## 🌎 Overview do Repositório
- [Objetivo do Projeto](#objetivo-do-projeto)
- [Modelos](#modelos)
- [Datasets Utilizados](#datasets-utilizados)
- [Estrutura do Repositório](#estrutura-do-repositorio)
- [Tecnologias Utilizadas](#tecnologias-utilizadas)
- [Como Executar o Projeto](#como-executar-o-projeto)
- [Resultado das Queries](#resultado-das-queries)


## 🎯 Objetivo do Projeto

Este repositório contém o Projeto 2 desenvolvido para a disciplina MC536 – Banco de Dados da Unicamp (1º semestre de 2025). Ele é uma continuação e refatoração do [Projeto 1](https://github.com/mariagabi-lustosa/projeto_1_MC536/tree/main), no qual foi utilizada modelagem relacional em PostgreSQL.

Neste segundo projeto, os dados foram reorganizados para um banco de dados orientado a grafos, utilizando o Neo4j. Essa mudança permitiu explorar relações mais complexas entre as entidades (como cursos, instituições, áreas de atuação e empregos), possibilitando análises mais eficientes e sofisticadas, que a modelagem relacional não permite representar.

Houve a possibilidade de decidir entre três diferentes bancos de dados para realizar a refatoração indo do modelo relacional para um não relacional. Dentre MOngoDB, DuckDB e Neo4j, optamos pelo último. Isso se deu levando em consideração alguns aspectos:

- **Forma de armazenamento de arquivos**: a organização dos dados em nós representando as entidades e arestas, os relacionamentos, é adequada ao cenário centrado em relacionamentos, os quais precisam ser modelados de forma eficiente. Uma vantagem da modelagem por grafos é justamente essa: geralmente conseguimos extrair informações mais relevantes das areas que dos nós.

- **Linguagem de processamento de consultas**: o Neo4j usa a lingaugem Cypher, que é pensada para consultas em grafos. Sua sintaxe é adequada para percorrer caminhos e acessar relacionamentos complexos, o que é essencial para as queries propostas neste projeto, visto que envolvem conexões em vários níveis.

- **Processamento e controle de transações**: há suporte à ACID (Atomicity, Consistency, Isolation, Durability), o que garante a integridade relacional das operações. Isso é importante para esse projeto, pois ele envolve a manipulação de dados interdependentes e sensíveis à consistência.

- **Mecanismos de recuperação e segurança**: Neo4j tem suporte robusto a controle de acesso, logs transacionais e backups automáticos, além de funcionalidades específicas para auditoria e rastreamento de alterações nos dados.


## 🧠 Modelo

### Modelo Lógico de Grafos
![Modelo de Grafos](models/projeto_neo4j.svg)


## 📊 Datasets Utilizados

Este projeto utiliza dados provenientes de duas principais fontes públicas nacionais: RAIS (Relação Anual de Informações Sociais) e Censo da Educação Superior (Inep), abrangendo o período de 2020 a 2023. Os dados foram selecionados e organizados de modo a possibilitar análises combinadas entre mercado de trabalho e formação acadêmica no Brasil.

#### Datasets Originais (`/datasets`)
- Dados do Censo da Educação Superior (Inep): **indicadores_educacao.csv**
- Dados RAIS sobre empregos e remuneração: **rais_tabela4_joined.csv** e **rais_tabela6_joined.csv**

#### Datasets Processados (`/preprocessed_dataset`)
- Arquivos pré-processados e compatibilizados para uso no Neo4j
- Incluem dados de 2021 e 2023, tanto para a Tabela 4 (empregos formais) quanto para a Tabela 6 (remuneração)


## 🗂️ Estrutura do Repositório

```
📦 projeto_2_MC536
├── 📁 datasets
│   ├── indicadores_educacao.csv
│   ├── rais_tabela4_joined.csv
│   ├── rais_tabela6_joined.csv
├── 📁 models
│   └── projeto_neo4j.svg
├── 📁 preprocessed_dataset
│   ├── indicadores_trajetoria_educacao_superior_2019_2023.csv
│   ├── RAIS_ano_base_2021_TABELA4.csv
│   ├── RAIS_ano_base_2021_TABELA6.csv
│   ├── RAIS_ano_base_2023_TABELA4.csv
│   └── RAIS_ano_base_2023_TABELA6.csv
├── 📁 python_files
│   ├── create_and_fill_database.py
│   ├── process_datasets.py
│   └── queries.py
├── 📁 queries
│   ├── query_1_result.csv
│   ├── query_2_result.csv
│   ├── query_3_result.csv
│   ├── query_4_result.csv
│   ├── query_5_result.csv
│   ├── query_6_result.csv
│   └── query_7_result.csv
├── requirements.txt
└── README.md
```
 

## 🛠️ Tecnologias Utilizadas

**Banco de Dados:** `Neo4j>=5.10`

**Linguagem de Programação:** `Python==3.12.7`

**Bibliotecas Python:**
```
pandas==2.2.3
numpy==2.2.5
neo4j==5.28.1
argparse==1.1
rapidfuzz==3.13.0
```

**Ferramenta:** `Neo4j Desktop` para visualização e execução de queries


## ⚙️ Como Executar o Projeto

### 1. Clonar o Repositório
```bash
git clone https://github.com/mariagabi-lustosa/projeto_2_MC536.git
cd projeto_2_MC536
```

### 2. (Opcional) Criar Ambiente Virtual
```bash
python -m venv venv
source venv/bin/activate
```

### 3. Instalar Dependências
```bash
pip install -r requirements.txt
```

### 4. Executar o Pré-processamento dos Dados
```bash
python python_files/process_datasets.py
```

### 5. Criar e Popular o Banco de Dados no Neo4j
Configure a URL, usuário e senha do seu banco Neo4j no script `create_and_fill_database.py`, e execute:
```bash
python python_files/create_and_fill_database.py
```

### 6. Rodar as Consultas em Grafos
```bash
python python_files/queries.py
```


## 📈 Resultado das Queries

| Query | Descrição |
|-------|-----------|
| `query_1_result.csv` | Relações entre cursos e áreas de atuação com maiores ofertas de emprego |
| `query_2_result.csv` | Instituições com maior proporção de concluintes em áreas em ascensão |
| `query_3_result.csv` | Relações entre municípios e áreas com melhor remuneração |
| `query_4_result.csv` | Comparação entre anos de variação de remuneração por UF |
| `query_5_result.csv` | Percurso educacional típico de um estudante até o mercado |
| `query_6_result.csv` | Relações entre cursos e evasão com contexto de mercado |
| `query_7_result.csv` | Conectividade entre instituições, áreas e remuneração |


## 📄 Licença
Este projeto é de uso acadêmico e está sujeito às diretrizes da disciplina MC536 da Unicamp oferecida no primeiro semestre de 2025.
