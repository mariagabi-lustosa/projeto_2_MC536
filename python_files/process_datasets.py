import pandas as pd
import argparse
import os
import re
import unicodedata
from rapidfuzz import fuzz, process

lookup_df = pd.DataFrame({
    'CO_UF': [
        12, 27, 13, 16, 29, 23, 53, 32, 52, 21, 31, 50,
        51, 15, 25, 26, 22, 41, 33, 24, 43, 11, 14, 42,
        28, 35, 17
    ],
    'uf_sigla': [
        'AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS',
        'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC',
        'SE', 'SP', 'TO'
    ],
    'uf_nome': [
        'Acre', 'Alagoas', 'Amazonas', 'Amapá', 'Bahia', 'Ceará', 'Distrito Federal', 'Espírito Santo',
        'Goiás', 'Maranhão', 'Minas Gerais', 'Mato Grosso do Sul', 'Mato Grosso', 'Pará',
        'Paraíba', 'Pernambuco', 'Piauí', 'Paraná', 'Rio de Janeiro', 'Rio Grande do Norte',
        'Rio Grande do Sul', 'Rondônia', 'Roraima', 'Santa Catarina', 'Sergipe',
        'São Paulo', 'Tocantins'
    ]
})

def normalize_string(s):
    """ Normalize a string by removing special characters and converting to lowercase.
    Args:
        s: The string to normalize.
    """
    if pd.isnull(s):
        return ''
    s = str(s).lower()
    s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('utf-8')
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def match_name(name, choices, scorer=fuzz.token_sort_ratio):
    """ Match a name to a list of choices using fuzzy matching.
    Args:
        name: The name to match.
        choices: The list of choices to match against.
        scorer: The scoring function to use for matching.
    """
    match = process.extractOne(name, choices, scorer=scorer)
    if match:
        return match
    else:
        return (None, 0, None)

def fix_value(val):
    """ Fix the value by removing dots and converting to integer.
    
    Args:
        val: The value to fix.
    """
    try:
        val_str = str(val)
        parts = val_str.split('.')
        if len(parts) == 2:
            integer_part, decimal_part = parts
            if len(decimal_part) > 1:
                return int(integer_part + decimal_part)
            else:
                return int(float(val))
        else:
            # Handle unexpected formats like '1.436.0' by removing all dots
            return int(val_str.replace('.', ''))
    except Exception as e:
        print(f"Error with value {val}: {e}")
        return None  # or raise, or some default


def process_indicadores(indicadores_csv, output_csv):
    """ Process the indicadores CSV file and save it to a new location.

    Args:
        indicadores_csv: Path to the CSV file containing the indicadores data.
        output_csv: Path to the directory where the processed CSV file will be saved.
    """

    # Read the CSV file
    df = pd.read_csv(
        indicadores_csv, 
        delimiter=',', 
        encoding='utf-8',
        skiprows=8,
        low_memory=False,
        header=0
    )

    # Match the UF codes with the lookup table
    df = df.merge(lookup_df, on='CO_UF', how='left')
    
    # Delete unnecessary columns
    df.drop(columns=['CO_REGIAO', 'CO_UF', 'CO_CINE_ROTULO', 'NO_CINE_ROTULO', 'NU_ANO_INGRESSO', 'NU_PRAZO_INTEGRALIZACAO', 'NU_ANO_INTEGRALIZACAO', 'NU_PRAZO_ACOMPANHAMENTO', 'NU_ANO_MAXIMO_ACOMPANHAMENTO', 'QT_PERMANENCIA', 'QT_DESISTENCIA', 'QT_FALECIDO', 'TAP', 'TCA', 'TCAN', 'TADA'], inplace=True)

    # Rename columns
    df.rename(columns={
        'CO_IES': 'inst_cod',
        'NO_IES': 'inst_nome',
        'TP_CATEGORIA_ADMINISTRATIVA': 'categoria_adm',
        'TP_ORGANIZACAO_ACADEMICA': 'org_academica',
        'CO_CURSO': 'curso_cod',
        'NO_CURSO': 'curso_nome',
        'CO_MUNICIPIO': 'municipio_cod',
        'TP_GRAU_ACADEMICO': 'grau_academico',
        'TP_MODALIDADE_ENSINO': 'modo_ensino',
        'CO_CINE_AREA_GERAL': 'area_cod',
        'NO_CINE_AREA_GERAL': 'nome_area_atuacao',
        'NU_ANO_REFERENCIA': 'ano_referencia',
        'QT_INGRESSANTE': 'num_ingressantes',
        'QT_CONCLUINTE': 'num_concluintes',
        'TDA': 'taxa_desistencia'
    }, inplace=True)

    # Save the processed DataFrame to a new CSV file

    df = df.dropna()

    df['inst_cod'] = df['inst_cod'].astype(int)
    df['categoria_adm'] = df['categoria_adm'].astype(int)
    df['org_academica'] = df['org_academica'].astype(int)
    df['curso_cod'] = df['curso_cod'].astype(int)
    df['municipio_cod'] = df['municipio_cod'].astype(int)
    df['grau_academico'] = df['grau_academico'].astype(int)
    df['modo_ensino'] = df['modo_ensino'].astype(int)
    df['area_cod'] = df['area_cod'].astype(int)
    df['ano_referencia'] = df['ano_referencia'].astype(int)
    df['num_ingressantes'] = df['num_ingressantes'].astype(int)
    df['num_concluintes'] = df['num_concluintes'].astype(int)
    df['taxa_desistencia'] = df['taxa_desistencia'].str.replace(',', '.', regex=False)

    output_file = os.path.join(output_csv, 'indicadores_educacao.csv')

    if os.path.exists(output_file):
        open(output_file, 'w').close()

    df.to_csv(output_file, index=False, sep=';')

    return 


def process_rais_4_2021(tabela4_csv, output_csv):
    """ Process the RAIS Tabela 4 2021 CSV file and save it to a new location.
    Args:
        tabela4_csv: Path to the CSV file containing the RAIS Tabela 4 data.
        output_csv: Path to the directory where the processed CSV file will be saved.
    """

    df = pd.read_csv(
        tabela4_csv, 
        delimiter=',', 
        encoding='utf-8',
        low_memory=False,
        skiprows=12,
        header=[0,1]
    )

    # Delete unnecessary columns
    df.drop(columns=['Unnamed: 0_level_0', 'Unnamed: 5_level_0', 'Unnamed: 6_level_0', 'Unnamed: 9_level_0', 'Unnamed: 10_level_0', 'Unnamed: 13_level_0', 'Unnamed: 14_level_0', 'Unnamed: 17_level_0', 'Unnamed: 18_level_0', 'Unnamed: 21_level_0', 'Unnamed: 22_level_0', 'Unnamed: 24_level_0', 'Unnamed: 25_level_0', 'Unnamed: 26_level_0', 'Total'], inplace=True)
    

    df.rename(columns={
        'Unnamed: 4_level_0': 'Agropecuária',
        'Unnamed: 8_level_0': 'Indústria',
        'Unnamed: 12_level_0': 'Construção',
        'Unnamed: 16_level_0': 'Comércio',
        '5 - Comércio': 'Serviços'
    }, inplace=True)

    # Select relevant columns
    df.columns.names = ['Type', 'Year'] 
    df.columns = [f"{type}_{year}" if type is not None else year for type, year in df.columns]

    df.rename(columns={
        'UF_Unnamed: 1_level_1': 'UF',
        'Município_Unnamed: 2_level_1': 'Município'
    }, inplace=True)

    df_melted = df.melt(
        id_vars=['UF', 'Município'],     
        var_name='Área_Ano',                
        value_name='Empregados'                   
    )

    df_melted[['Área', 'Ano']] = df_melted['Área_Ano'].str.split('_', expand=True)

    # Drop the original combined column
    df_melted = df_melted.drop(columns='Área_Ano')

    # Reorder columns
    df_melted = df_melted[['UF', 'Município', 'Área', 'Ano', 'Empregados']]

    df_melted.rename(columns={
        'UF': 'uf_sigla',
        'Município': 'municipio_nome',
        'Área': 'setor_nome',
        'Ano': 'ano',
        'Empregados': 'num_pessoas_empregadas'
    }, inplace=True)

    df_melted['num_pessoas_empregadas'] = df_melted['num_pessoas_empregadas'].str.replace(',', '', regex=False)

    df_melted = df_melted.dropna()

    output_file = os.path.join(output_csv, 'rais_tabela4_2021.csv')
    if os.path.exists(output_file):
        open(output_file, 'w').close()

    df_melted.to_csv(output_file, index=False, sep=';')

    return 


def process_rais_6_2021(tabela6_csv, output_csv):
    """ Process the RAIS Tabela 6 2021 CSV file and save it to a new location.
    Args:
        tabela6_csv: Path to the CSV file containing the RAIS Tabela 6 data.
        output_csv: Path to the directory where the processed CSV file will be saved.
    """
    df = pd.read_csv(
        tabela6_csv, 
        delimiter=',', 
        encoding='utf-8',
        low_memory=False,
        skiprows=11,
        header=0
    )

    df.drop(columns=['Unnamed: 0', 'Indicadores', 'Variação', 'Unnamed: 6'], inplace=True)

    df.rename(columns={
        'Unnamed: 2': 'Tipo',
        'Ano': '2020',
        'Unnamed: 4': '2021',

    }, inplace=True)

    macro_regions = {"Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"}
    
    is_region = df['Tipo'].isin(macro_regions)

    is_region.index = df.index

    df_uf = df[~is_region]

    stop_uf = df_uf[df_uf['Tipo'] == 'Agricultura, pecuária, produção florestal, pesca e aquicultura'].index

    if not stop_uf.empty:
        df_uf = df_uf.loc[:stop_uf[0]-1]

    df_uf = df_uf.dropna()

    df_uf = df_uf.melt(
        id_vars=['Tipo'],
        var_name='Ano',
        value_name='Renumeração'
    )

    df_uf.rename(columns={
        'Tipo': 'uf_nome',
        'Ano': 'ano',
        'Renumeração': 'media_remuneracao'
    }, inplace=True)

    df_uf['media_remuneracao'] = df_uf['media_remuneracao'].str.replace(',', '', regex=False)

    output_file_uf = os.path.join(output_csv, 'rais_tabela6_2021.csv')
    if os.path.exists(output_file_uf):
        open(output_file_uf, 'w').close()

    df_uf.to_csv(output_file_uf, index=False, sep=';')

    return


def process_rais_4_2023(tabela4_csv, output_csv):
    """ Process the RAIS Tabela 4 2023 CSV file and save it to a new location.

    Args:
        tabela4_csv: Path to the CSV file containing the RAIS Tabela 4 data.
        output_csv: Path to the directory where the processed CSV file will be saved.
    """
    df = pd.read_csv(
        tabela4_csv, 
        delimiter=',', 
        encoding='utf-8',
        low_memory=False,
        skiprows=12,
        header=[0,1]
    )

    # Delete unnecessary columns
    df.drop(columns=['Unnamed: 0_level_0', 'Unnamed: 6_level_0', 'Unnamed: 7_level_0', 'Unnamed: 10_level_0', 'Unnamed: 11_level_0', 'Unnamed: 14_level_0', 'Unnamed: 15_level_0', 'Unnamed: 18_level_0', 'Unnamed: 19_level_0', 'Unnamed: 22_level_0', 'Unnamed: 23_level_0', 'Unnamed: 25_level_0', 'Unnamed: 26_level_0', 'Unnamed: 27_level_0', 'Total'], inplace=True)

    # Rename columns
    df.rename(columns={
        'Unnamed: 5_level_0': 'Agropecuária',
        'Unnamed: 9_level_0': 'Indústria',
        'Unnamed: 13_level_0': 'Construção',
        'Unnamed: 17_level_0': 'Comércio',
        'Unnamed: 21_level_0': 'Serviços'
    }, inplace=True)

    # Select relevant columns
    df.columns.names = ['Type', 'Year'] 
    df.columns = [f"{type}_{year}" if type is not None else year for type, year in df.columns]

    df.rename(columns={
        'UF_Unnamed: 1_level_1': 'UF',
        'Código_Unnamed: 2_level_1': 'Código',
        'Município_Unnamed: 3_level_1': 'Município'
    }, inplace=True)

    df_melted = df.melt(
        id_vars=['UF','Código', 'Município'],     
        var_name='Área_Ano',                
        value_name='Empregados'                   
    )

    df_melted[['Área', 'Ano']] = df_melted['Área_Ano'].str.split('_', expand=True)

    # Drop the original combined column
    df_melted = df_melted.drop(columns='Área_Ano')

    # Reorder columns
    df_melted = df_melted[['UF', 'Código', 'Município', 'Área', 'Ano', 'Empregados']]

    df_melted.rename(columns={
        'UF': 'uf_sigla',
        'Código': 'municipio_cod',
        'Município': 'municipio_nome',
        'Área': 'setor_nome',
        'Ano': 'ano',
        'Empregados': 'num_pessoas_empregadas'
    }, inplace=True)

    df_melted = df_melted.dropna()

    df_melted['num_pessoas_empregadas'] = df_melted['num_pessoas_empregadas'].apply(fix_value)


    # Save the processed DataFrame to a new CSV file
    output_file = os.path.join(output_csv, 'rais_tabela4_2023.csv')
    if os.path.exists(output_file):
        open(output_file, 'w').close()

    df_melted.to_csv(output_file, index=False, sep=';')

    return


def process_rais_6_2023(tabela6_csv, output_csv):
    """ Process the RAIS Tabela 6 2023 CSV file and save it to a new location.

    Args:
        tabela6_csv: Path to the CSV file containing the RAIS Tabela 6 data.
        output_csv: Path to the directory where the processed CSV file will be saved.
    """
    df = pd.read_csv(
        tabela6_csv, 
        delimiter=',', 
        encoding='utf-8',
        low_memory=False,
        skiprows=11,
        header=0,
    )

    df.drop(columns=['Unnamed: 0', 'Indicadores', 'Variação Absoluta', 'Variação Relativa'], inplace=True)
    df.rename(columns={
        'Unnamed: 2': 'Tipo'
    }, inplace=True)

    macro_regions = { "Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul" }
    
    is_region = df['Tipo'].isin(macro_regions)

    df_uf = df[~is_region]

    stop_uf = df[df['Tipo'] == 'Agricultura, pecuária, produção florestal, pesca e aquicultura'].index

    if not stop_uf.empty:
        df_uf = df_uf.loc[:stop_uf[0]-1]

    df_uf = df_uf.dropna()

    df_uf = df_uf.melt(
        id_vars=['Tipo'],
        var_name='Ano',
        value_name='Renumeração'
    )

    df_uf.rename(columns={
        'Tipo': 'uf_nome',
        'Ano': 'ano',
        'Renumeração': 'media_remuneracao'
    }, inplace=True)

    df_uf['media_remuneracao'] = df_uf['media_remuneracao'].str.replace('.', '', regex=False)
    df_uf['media_remuneracao'] = df_uf['media_remuneracao'].str.replace(',', '.', regex=False)

    output_file_uf = os.path.join(output_csv, 'rais_tabela6_2023.csv')
    if os.path.exists(output_file_uf):
        open(output_file_uf, 'w').close()

    df_uf.to_csv(output_file_uf, index=False, sep=';')

    return


def join_rais_4(rais_4_2021, rais_4_2023, output_csv):
    """ Join the RAIS Tabela 4 2021 and 2023 CSV files and save it to a new location.

    Args:
        rais_4_2021: Path to the RAIS Tabela 4 2021 CSV file.
        rais_4_2023: Path to the RAIS Tabela 4 2023 CSV file.
        output_csv: Path to the directory where the processed CSV file will be saved.
    """
    df_2021 = pd.read_csv(rais_4_2021, sep=';')
    df_2023 = pd.read_csv(rais_4_2023, sep=';')

    df_2021['name_norm'] = df_2021['municipio_nome'].apply(normalize_string)
    df_2023['name_norm'] = df_2023['municipio_nome'].apply(normalize_string)

    df_2021['matched_index'] = None
    df_2021['match_score'] = None

    for idx, row in df_2021.iterrows():
        uf = row['uf_sigla']
        name_norm = row['name_norm']

        candidates = df_2023[df_2023['uf_sigla'] == uf]

        if not candidates.empty:
            match = process.extractOne(
                name_norm, 
                candidates['name_norm'],
                scorer=fuzz.token_sort_ratio
            )
            if match:
                best_match, score, index = match
                matched_row = candidates.loc[index]
                df_2021.at[idx, 'matched_index'] = matched_row.name
                df_2021.at[idx, 'match_score'] = score
    
    df_2021['municipio_nome'] = df_2021['matched_index'].apply(lambda idx: df_2023.loc[idx, 'municipio_nome'] if pd.notnull(idx) else None)
    df_2021['municipio_cod'] = df_2021['matched_index'].apply(lambda idx: df_2023.loc[idx, 'municipio_cod'] if pd.notnull(idx) else None)

    df_2021 = df_2021[['uf_sigla', 'municipio_cod', 'municipio_nome', 'setor_nome', 'ano', 'num_pessoas_empregadas']]
    df_2023 = df_2023[['uf_sigla', 'municipio_cod', 'municipio_nome', 'setor_nome', 'ano', 'num_pessoas_empregadas']]

    final_df = pd.concat([df_2021, df_2023], ignore_index=True)
    final_df = final_df[final_df['uf_sigla'] != 'NI']
    final_df = final_df.dropna()

    final_df['municipio_cod'] = final_df['municipio_cod'].astype(int)
    final_df['ano'] = final_df['ano'].astype(int)
    final_df['num_pessoas_empregadas'] = final_df['num_pessoas_empregadas'].astype(int)

    output_file = os.path.join(output_csv, 'rais_tabela4_joined.csv')
    if os.path.exists(output_file):
        open(output_file, 'w').close()

    final_df.to_csv(output_file, index=False, sep=';')

    return
    

def join_rais_6(rais_6_2021, rais_6_2023, output_csv):
    """ Join the RAIS Tabela 6 2021 and 2023 CSV files and save it to a new location.

    Args:
        rais_6_2021: Path to the RAIS Tabela 6 2021 CSV file.
        rais_6_2023: Path to the RAIS Tabela 6 2023 CSV file.
        output_csv: Path to the directory where the processed CSV file will be saved.
    """
    df_rais_6_2021 = pd.read_csv(rais_6_2021, sep=';')
    df_rais_6_2023 = pd.read_csv(rais_6_2023, sep=';')

    df_joined = pd.concat([df_rais_6_2021, df_rais_6_2023], ignore_index=True)

    df_joined = df_joined.merge(lookup_df[['uf_nome', 'uf_sigla']], on='uf_nome', how='left')

    df_joined['ano'] = df_joined['ano'].astype(int)

    output_file = os.path.join(output_csv, 'rais_tabela6_joined.csv')
    if os.path.exists(output_file):
        open(output_file, 'w').close()

    df_joined.to_csv(output_file, index=False, sep=';')

    return


def main(indicadores_csv, rais_4_csv, rais_6_csv, bool_rais4, bool_rais6, output_csv):
    """ Main function to process the CSV files.

    Args:
        indicadores_csv: Path to the CSV file containing the indicadores data.
        rais_4_csv: Path to the CSV file containing the RAIS Tabela 4 data.
        rais_6_csv: Path to the CSV file containing the RAIS Tabela 6 data.
        bool_rais4: Boolean to recreate the RAIS Tabela 4 data.
        bool_rais6: Boolean to recreate the RAIS Tabela 6 data.
        output_csv: Path to the directory where the processed CSV file will be saved.
    """
    if indicadores_csv is not None and output_csv is not None:
        process_indicadores(indicadores_csv, output_csv)
    if rais_4_csv is not None and output_csv is not None:
        if "2021" in rais_4_csv:
            process_rais_4_2021(rais_4_csv, output_csv)
        if "2023" in rais_4_csv:
            process_rais_4_2023(rais_4_csv, output_csv)
    if rais_6_csv is not None and output_csv is not None:
        if "2021" in rais_6_csv:
            process_rais_6_2021(rais_6_csv, output_csv)
        if "2023" in rais_6_csv:
            process_rais_6_2023(rais_6_csv, output_csv)

    rais_4_2021 = f"{output_csv}/rais_tabela4_2021.csv"
    rais_4_2023 = f"{output_csv}/rais_tabela4_2023.csv"
    rais_6_2021 = f"{output_csv}/rais_tabela6_2021.csv"
    rais_6_2023 = f"{output_csv}/rais_tabela6_2023.csv"

    # Check if the files exist
    if os.path.exists(rais_4_2021) and os.path.exists(rais_4_2023) and bool_rais4 == "True":
        join_rais_4(rais_4_2021, rais_4_2023, output_csv)
    if os.path.exists(rais_6_2021) and os.path.exists(rais_6_2023) and bool_rais6 == "True":
        join_rais_6(rais_6_2021, rais_6_2023, output_csv)
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process CSV files relevants to the project."
    )
    parser.add_argument(
        "-i",
        "--indicadores",
        default=None,
        help="Path to the CSV file containing the indicadores data."
    )
    parser.add_argument(
        "-r4",
        "--rais_4",
        default=None,
        help="Path to the CSV file containing the RAIS Tabela 4 data."
    )
    parser.add_argument(
        "-r6",
        "--rais_6",
        default=None,
        help="Path to the CSV file containing the RAIS Tabela 6 data."
    )
    parser.add_argument(
        "-b4",
        "--bool_rais4",
        default=False,
        help="Boolean to recreate the RAIS Tabela 4 data."
    )
    parser.add_argument(
        "-b6",
        "--bool_rais6",
        default=False,
        help="Boolean to recreate the RAIS Tabela 6 data."
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Path to the directory where the processed CSV file will be saved."
    )
    args = parser.parse_args()
    indicadores_csv = args.indicadores
    rais_4_csv = args.rais_4
    rais_6_csv = args.rais_6
    bool_rais4 = args.bool_rais4
    bool_rais6 = args.bool_rais6
    output_csv = args.output

    main(indicadores_csv, rais_4_csv, rais_6_csv, bool_rais4, bool_rais6, output_csv)
