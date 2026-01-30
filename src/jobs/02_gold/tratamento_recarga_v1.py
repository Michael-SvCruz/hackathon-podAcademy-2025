"""
Tratamento e Enriquecimento da Base de Recarga
===============================================
Script para processar a base RECARGA com tratamentos especiais para:
- Enriquecimento com dimensões (Forma Pagamento, Instituição, Status, etc.)
- Ajuste de valores com reembolsos (SOS)
- Agregação mensal com métricas de tempo entre recargas
- Classificação de tipos de transação

Autor: Squad Data Engineering
Data: 2026-01-27
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType


def enriquecer_recarga(
    df_base: DataFrame,
    nome_arquivo: str,
    coluna_chave: str,
    colunas_trazer: list,
    caminho_base: str = '/Volumes/workspace/hackathon_2025/default/source/bases_recarga/'
) -> DataFrame:
    """
    Enriquece o dataframe base com dados de arquivos CSV via left join.
    Trata encoding UTF-8 com problemas de acentuação.
    
    Args:
        df_base: DataFrame principal (recarga)
        nome_arquivo: Nome do arquivo CSV sem extensão
        coluna_chave: Coluna para fazer o join
        colunas_trazer: Lista de colunas a trazer do CSV
        caminho_base: Caminho do diretório com os CSVs
    
    Returns:
        DataFrame enriquecido
    """
    
    # Ler CSV com encoding correto
    df_mapeamento = spark.read.csv(
        f'{caminho_base}{nome_arquivo}.csv',
        header=True,
        inferSchema=True,
        sep=',',
        encoding='ISO-8859-1'  # Corrige problema de acentuação
    )
    
    # Selecionar apenas colunas necessárias
    df_mapeamento_select = df_mapeamento.select([coluna_chave] + colunas_trazer)
    
    # Remover duplicatas na chave do mapeamento (garantia)
    df_mapeamento_select = df_mapeamento_select.dropDuplicates([coluna_chave])
    
    # Join
    df_enriquecido = df_base.join(
        df_mapeamento_select,
        on=coluna_chave,
        how='left'
    )
    
    # Validação: verificar % de match
    total_registros = df_base.count()
    registros_com_match = df_enriquecido.filter(F.col(colunas_trazer[0]).isNotNull()).count()
    match_percent = (registros_com_match / total_registros * 100) if total_registros > 0 else 0
    
    print(f"✓ Arquivo: {nome_arquivo}")
    print(f"✓ Join em: '{coluna_chave}'")
    print(f"✓ Colunas adicionadas: {', '.join(colunas_trazer)}")
    print(f"✓ Match rate: {match_percent:.2f}% ({registros_com_match:,}/{total_registros:,})")
    
    if match_percent < 90:
        print(f"⚠️ Atenção: {100-match_percent:.2f}% dos registros não tiveram match!")
    
    print("-" * 80)
    
    return df_enriquecido


def processar_recarga_base():
    """
    Etapa 1: Carregar base de recarga e enriquecer com dimensões.
    """
    print("\n" + "="*80)
    print("ETAPA 1: CARREGAMENTO E ENRIQUECIMENTO DA BASE RECARGA")
    print("="*80 + "\n")
    
    # Carregar base principal
    recarga = spark.read.parquet("/Volumes/workspace/hackathon_2025/default/source/bases_recarga/BI_FP_ASS_RECARGA_CMV_NOVA/")
    
    # Aplicar todos os enriquecimentos sequencialmente
    print("="*80)
    print("INICIANDO ENRIQUECIMENTO DA BASE RECARGA")
    print("="*80)

    # 1. Forma de Pagamento
    recarga = enriquecer_recarga(
        df_base=recarga,
        nome_arquivo='BI_DIM_FORMA_PAGAMENTO',
        coluna_chave='DW_FORMA_PAGAMENTO',
        colunas_trazer=['DSC_FORMA_PAGAMENTO']
    )

    # 2. Instituição
    recarga = enriquecer_recarga(
        df_base=recarga,
        nome_arquivo='BI_DIM_INSTITUICAO',
        coluna_chave='DW_INSTITUICAO',
        colunas_trazer=['DSC_TIPO_INSTITUICAO']
    )

    # 3. Status Plataforma
    recarga = enriquecer_recarga(
        df_base=recarga,
        nome_arquivo='BI_DIM_STATUS_PLATAFORMA',
        coluna_chave='COD_STATUS_PLATAFORMA',
        colunas_trazer=['DSC_STATUS_PLATAFORMA']
    )

    # 4. Tipo de Crédito
    recarga = enriquecer_recarga(
        df_base=recarga,
        nome_arquivo='BI_DIM_TIPO_CREDITO',
        coluna_chave='COD_TIPO_CREDITO',
        colunas_trazer=['DSC_TIPO_CREDITO']
    )

    # 5. Tipo de Inserção
    recarga = enriquecer_recarga(
        df_base=recarga,
        nome_arquivo='BI_DIM_TIPO_INSERCAO',
        coluna_chave='DW_TIPO_INSERCAO',
        colunas_trazer=['DSC_TIPO_INSERCAO']
    )

    # 6. Tipo de Recarga
    recarga = enriquecer_recarga(
        df_base=recarga,
        nome_arquivo='BI_DIM_TIPO_RECARGA',
        coluna_chave='DW_TIPO_RECARGA',
        colunas_trazer=['DSC_TIPO_RECARGA']
    )

    print("="*80)
    print("ENRIQUECIMENTO CONCLUÍDO!")
    print(f"Total de colunas no DataFrame final: {len(recarga.columns)}")
    print("="*80)
    
    return recarga


def processar_datas_e_tipos(recarga: DataFrame) -> DataFrame:
    """
    Etapa 2: Processar datas e converter tipos, criar flags de classificação.
    """
    print("\n" + "="*80)
    print("ETAPA 2: PROCESSAMENTO DE DATAS E TIPOS DE VALORES")
    print("="*80 + "\n")
    
    # Criar coluna DATA_INSERCAO no formato yyyy-MM-dd
    recarga = recarga.withColumn(
        'DATA_INSERCAO',
        F.to_date(
            F.substring(F.col('DAT_INSERCAO_CREDITO'), 1, 9),
            'ddMMMyyyy'
        )
    )
    
    # Converter colunas para Double
    recarga = recarga.withColumn('VAL_CREDITO_INSERIDO', F.col('VAL_CREDITO_INSERIDO').cast(DoubleType())) \
                     .withColumn('VAL_BONUS', F.col('VAL_BONUS').cast(DoubleType())) \
                     .withColumn('VAL_REAL', F.col('VAL_REAL').cast(DoubleType())) \
                     .withColumn('VALOR_SOS', 
                                F.when(F.col('VALOR_SOS').isNull(), 0.0)
                                 .otherwise(F.col('VALOR_SOS').cast(DoubleType())))

    # Criar flags de classificação de tipo de valor
    recarga = recarga.withColumn(
        'FLAG_TIPO_VALOR',
        F.when((F.col('VAL_CREDITO_INSERIDO') > 0) & (F.col('VAL_BONUS') == 0), 'PAGO_PURO')
         .when((F.col('VAL_CREDITO_INSERIDO') == 0) & (F.col('VAL_BONUS') > 0), 'BONUS_PURO')
         .when((F.col('VAL_CREDITO_INSERIDO') > 0) & (F.col('VAL_BONUS') > 0), 'COMBO_PAGO_BONUS')
         .when((F.col('VAL_CREDITO_INSERIDO') == 0) & (F.col('VAL_BONUS') == 0) & (F.col('VAL_REAL') == 0), 'ZERO_TOTAL')
         .when(F.col('VAL_REAL') < 0, 'VALOR_NEGATIVO')
         .when((F.col('VAL_BONUS') < 0) | (F.col('VAL_CREDITO_INSERIDO') < 0), 'COMPONENTE_NEGATIVO')
         .otherwise('INCONSISTENTE')
    )

    # Ajustar VAL_REAL quando há SOS
    recarga = recarga.withColumn(
        'VAL_REAL_AJUSTADO',
        F.when(
            (F.col('FLAG_SOS').cast('int') == 1) & (F.col('VALOR_SOS') == F.col('VAL_CREDITO_INSERIDO')),
            -F.col('VALOR_SOS')  # Se SOS = valor pago, VAL_REAL fica negativo
        )
        .when(
            (F.col('FLAG_SOS').cast('int') == 1) & (F.col('VALOR_SOS') != F.col('VAL_CREDITO_INSERIDO')),
            F.col('VAL_CREDITO_INSERIDO') - F.col('VALOR_SOS')  # Se diferente, desconta
        )
        .otherwise(F.col('VAL_REAL'))  # Mantém valor original
    )

    # Ajustar VAL_REAL_AJUSTADO retirando bônus quando necessário
    recarga = recarga.withColumn(
        'VAL_REAL_AJUSTADO_FINAL',
        F.when(
            F.col('FLAG_TIPO_VALOR').isin(['COMBO_PAGO_BONUS', 'BONUS_PURO']),
            F.col('VAL_REAL_AJUSTADO') - F.col('VAL_BONUS')  # Retira bônus
        ).otherwise(
            F.col('VAL_REAL_AJUSTADO')  # Mantém como está
        )
    )

    print("✓ Conversão de tipos e ajustes de SOS/Bônus concluídos")
    
    return recarga


def processar_temporal(recarga: DataFrame) -> DataFrame:
    """
    Etapa 3: Criar timestamp completo e calcular métrica temporal.
    """
    print("\n" + "="*80)
    print("ETAPA 3: PROCESSAMENTO TEMPORAL - CÁLCULO DE TEMPO ENTRE RECARGAS")
    print("="*80 + "\n")
    
    # Criar hora normalizada para cálculos precisos de tempo
    recarga = recarga.withColumn(
        'HORA_NORMALIZADA',
        F.lpad(F.col('HOR_INSERCAO_CREDITO'), 6, '0')
    ).withColumn(
        'TIMESTAMP_COMPLETO',
        F.to_timestamp(
            F.concat(
                F.col('DATA_INSERCAO'),
                F.lit(' '),
                F.substring(F.col('HORA_NORMALIZADA'), 1, 2), F.lit(':'),
                F.substring(F.col('HORA_NORMALIZADA'), 3, 2), F.lit(':'),
                F.substring(F.col('HORA_NORMALIZADA'), 5, 2)
            ),
            'yyyy-MM-dd HH:mm:ss'
        )
    )

    # Flag de recarga válida (exclui apenas ZERO_TOTAL)
    recarga = recarga.withColumn(
        'FLAG_RECARGA_VALIDA',
        F.when(F.col('FLAG_TIPO_VALOR') != 'ZERO_TOTAL', 1).otherwise(0)
    )

    # Window para calcular tempo entre recargas VÁLIDAS (por CPF + telefone)
    window_tempo = Window.partitionBy('NUM_CPF', 'DW_NUM_NTC') \
                         .orderBy('TIMESTAMP_COMPLETO')

    recarga = recarga.withColumn(
        'TIMESTAMP_RECARGA_ANTERIOR',
        F.lag('TIMESTAMP_COMPLETO', 1).over(window_tempo)
    ).withColumn(
        'FLAG_RECARGA_VALIDA_ANTERIOR',
        F.lag('FLAG_RECARGA_VALIDA', 1).over(window_tempo)
    )

    # Calcular diferença em horas e dias (apenas entre recargas válidas)
    recarga = recarga.withColumn(
        'HORAS_SEM_RECARGA',
        F.when(
            F.col('FLAG_RECARGA_VALIDA_ANTERIOR') == 1,
            (F.unix_timestamp('TIMESTAMP_COMPLETO') - F.unix_timestamp('TIMESTAMP_RECARGA_ANTERIOR')) / 3600
        ).otherwise(None)
    ).withColumn(
        'DIAS_SEM_RECARGA',
        F.round(F.col('HORAS_SEM_RECARGA') / 24, 2)
    )

    # Criar coluna de Ano-Mês para agregação
    recarga = recarga.withColumn(
        'ANO_MES',
        F.date_format(F.col('DATA_INSERCAO'), 'yyyy-MM')
    )

    print("✓ Cálculos de tempo entre recargas concluídos")
    print(f"✓ Total de registros: {recarga.count():,}")
    
    return recarga


def agregar_recarga_mensal(recarga: DataFrame) -> DataFrame:
    """
    Etapa 4: Criar agregação mensal por CPF.
    """
    print("\n" + "="*80)
    print("ETAPA 4: AGREGAÇÃO MENSAL POR CPF")
    print("="*80 + "\n")
    
    # Agregação mensal por CPF
    recarga_mensal = recarga.groupBy('NUM_CPF', 'ANO_MES').agg(
        
        # Quantidade de telefones distintos
        F.countDistinct('DW_NUM_NTC').alias('QTD_TELEFONES'),
        
        # Quantidade de recargas (APENAS VÁLIDAS)
        F.sum('FLAG_RECARGA_VALIDA').alias('QTD_RECARGAS_VALIDAS'),
        
        # Quantidade total de transações (incluindo zeros)
        F.count('*').alias('QTD_TRANSACOES_TOTAL'),
        
        # Métricas de SOS
        F.sum(F.when(F.col('FLAG_SOS').cast('int') == 1, 1).otherwise(0)).alias('QTD_SOS'),
        F.sum(F.when(F.col('FLAG_SOS').cast('int') == 1, F.col('VALOR_SOS')).otherwise(0)).alias('VALOR_SOS_MES'),
        
        # Valores financeiros
        F.sum('VAL_CREDITO_INSERIDO').alias('VAL_CREDITO_MES'),
        F.sum('VAL_BONUS').alias('VAL_BONUS_MES'),
        F.sum('VAL_REAL').alias('VAL_REAL_MES'),
        F.sum('VAL_REAL_AJUSTADO').alias('VAL_REAL_AJUSTADO_MES'),
        F.sum('VAL_REAL_AJUSTADO_FINAL').alias('VAL_REAL_AJUSTADO_FINAL_MES'),
        
        # Tempo sem recarga (em horas e dias) - apenas entre recargas válidas
        F.min('HORAS_SEM_RECARGA').alias('HORAS_MIN_SEM_RECARGA'),
        F.max('HORAS_SEM_RECARGA').alias('HORAS_MAX_SEM_RECARGA'),
        F.avg('HORAS_SEM_RECARGA').alias('HORAS_MEDIO_SEM_RECARGA'),
        
        F.min('DIAS_SEM_RECARGA').alias('DIAS_MIN_SEM_RECARGA'),
        F.max('DIAS_SEM_RECARGA').alias('DIAS_MAX_SEM_RECARGA'),
        F.avg('DIAS_SEM_RECARGA').alias('DIAS_MEDIO_SEM_RECARGA'),
        
        # Data da primeira e última recarga no mês
        F.min('DATA_INSERCAO').alias('DATA_PRIMEIRA_RECARGA'),
        F.max('DATA_INSERCAO').alias('DATA_ULTIMA_RECARGA')
        
    ).orderBy('NUM_CPF', 'ANO_MES')

    # Adicionar métricas derivadas
    recarga_mensal = recarga_mensal.withColumn(
        'TICKET_MEDIO',
        F.when(
            F.col('QTD_RECARGAS_VALIDAS') > 0,
            F.round(F.col('VAL_CREDITO_MES') / F.col('QTD_RECARGAS_VALIDAS'), 2)
        ).otherwise(0)
    ).withColumn(
        'FLAG_TEM_SOS_MES',
        F.when(F.col('QTD_SOS') > 0, 1).otherwise(0)
    ).withColumn(
        'PERC_SOS_VALOR',
        F.when(
            F.col('VAL_CREDITO_MES') > 0,
            F.round((F.col('VALOR_SOS_MES') / F.col('VAL_CREDITO_MES')) * 100, 2)
        ).otherwise(0)
    ).withColumn(
        'VALOR_LIQUIDO_MES',
        F.col('VAL_CREDITO_MES') - F.col('VALOR_SOS_MES')
    ).withColumn(
        'PERC_TRANSACOES_VALIDAS',
        F.round((F.col('QTD_RECARGAS_VALIDAS') / F.col('QTD_TRANSACOES_TOTAL')) * 100, 2)
    )

    print("✓ Agregação mensal criada")
    print(f"✓ Total de linhas agregadas: {recarga_mensal.count():,}")
    
    return recarga_mensal


def salvar_delta_e_registrar_tabela(recarga: DataFrame, recarga_mensal: DataFrame) -> None:
    """
    Etapa 5: Salvar DataFrames em Delta e registrar tabelas.
    """
    print("\n" + "="*80)
    print("ETAPA 5: SALVAMENTO EM DELTA E REGISTRO DE TABELAS")
    print("="*80 + "\n")
    
    # Salvar recarga (nível transacional)
    print("Salvando recarga (transacional) em Delta...")
    caminho_recarga = "/Volumes/hackathon_2025/default/gold/recarga_transacional_delta/"
    
    recarga.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("path", caminho_recarga) \
        .save()
    
    print(f"✓ Recarga (transacional) salva em: {caminho_recarga}")
    
    # Registrar tabela recarga como tabela gerenciada
    print("Registrando tabela recarga no Catalog...")
    spark.sql(f"""
        CREATE OR REPLACE TABLE hackathon_2025.default.recarga_transacional 
        USING DELTA 
        AS SELECT * FROM delta.`{caminho_recarga}`
    """)
    print("✓ Tabela recarga_transacional registrada")
    
    # Salvar recarga_mensal (agregado)
    print("\nSalvando recarga_mensal (agregado) em Delta...")
    caminho_recarga_mensal = "/Volumes/hackathon_2025/default/gold/recarga_mensal_delta/"
    
    recarga_mensal.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("ANO_MES") \
        .option("mergeSchema", "true") \
        .option("path", caminho_recarga_mensal) \
        .save()
    
    print(f"✓ Recarga (mensal) salva em: {caminho_recarga_mensal}")
    print("  (Particionada por ANO_MES)")
    
    # Registrar tabela recarga_mensal como tabela gerenciada
    print("Registrando tabela recarga_mensal no Catalog...")
    spark.sql(f"""
        CREATE OR REPLACE TABLE hackathon_2025.default.recarga_mensal 
        USING DELTA 
        AS SELECT * FROM delta.`{caminho_recarga_mensal}`
    """)
    print("✓ Tabela recarga_mensal registrada")
    
    print("\n" + "="*80)
    print("SALVAMENTO CONCLUÍDO COM SUCESSO!")
    print("="*80)
    
    return None


def validar_e_relatar(recarga: DataFrame, recarga_mensal: DataFrame) -> None:
    """
    Etapa 6: Validações e relatórios de qualidade.
    """
    print("\n" + "="*80)
    print("ETAPA 6: VALIDAÇÕES E RELATÓRIOS")
    print("="*80 + "\n")
    
    # Validação: Verificar ajuste de bônus
    print("="*100)
    print("VALIDAÇÃO: Comparação VAL_REAL_AJUSTADO vs VAL_REAL_AJUSTADO_FINAL")
    print("="*100 + "\n")

    recarga.filter(
        F.col('FLAG_TIPO_VALOR').isin(['COMBO_PAGO_BONUS', 'BONUS_PURO'])
    ).select(
        'NUM_CPF',
        'DATA_INSERCAO',
        'FLAG_TIPO_VALOR',
        'VAL_CREDITO_INSERIDO',
        'VAL_BONUS',
        'VAL_REAL',
        'VAL_REAL_AJUSTADO',
        'VAL_REAL_AJUSTADO_FINAL'
    ).show(20, truncate=False)

    # Verificar distribuição de tipos de transação
    print("\n" + "="*100)
    print("DISTRIBUIÇÃO DE TIPOS DE TRANSAÇÃO")
    print("="*100 + "\n")

    recarga.groupBy('FLAG_TIPO_VALOR').agg(
        F.count('*').alias('qtd'),
        F.sum('FLAG_RECARGA_VALIDA').alias('qtd_validas')
    ).orderBy(F.desc('qtd')).show()

    # Comparar totais antes e depois do ajuste de bônus
    print("\n" + "="*100)
    print("IMPACTO DO AJUSTE DE BÔNUS")
    print("="*100 + "\n")

    recarga_mensal.agg(
        F.sum('VAL_REAL_AJUSTADO_MES').alias('total_antes_ajuste_bonus'),
        F.sum('VAL_REAL_AJUSTADO_FINAL_MES').alias('total_apos_ajuste_bonus'),
        F.sum('VAL_BONUS_MES').alias('total_bonus_retirado')
    ).show(truncate=False)

    # Estatísticas gerais - Resumo por mês
    print("\n" + "="*100)
    print("ESTATÍSTICAS AGREGADAS MENSAIS - RESUMO GERAL POR MÊS")
    print("="*100 + "\n")

    recarga_mensal.groupBy('ANO_MES').agg(
        F.count('NUM_CPF').alias('qtd_clientes'),
        F.sum('QTD_RECARGAS_VALIDAS').alias('total_recargas_validas'),
        F.sum('QTD_TRANSACOES_TOTAL').alias('total_transacoes'),
        F.sum('QTD_SOS').alias('total_sos'),
        F.sum('VAL_CREDITO_MES').alias('receita_total'),
        F.sum('VAL_REAL_AJUSTADO_FINAL_MES').alias('valor_real_liquido')
    ).orderBy('ANO_MES').show(20)

    # Top 10 clientes por valor no mês
    print("\n" + "="*100)
    print("TOP 10 CLIENTES POR VALOR MENSAL")
    print("="*100 + "\n")

    recarga_mensal.orderBy(F.desc('VAL_CREDITO_MES')).select(
        'NUM_CPF',
        'ANO_MES',
        'QTD_TELEFONES',
        'QTD_RECARGAS_VALIDAS',
        'VAL_CREDITO_MES',
        'VAL_BONUS_MES',
        'VAL_REAL_AJUSTADO_FINAL_MES',
        'TICKET_MEDIO',
        'DIAS_MEDIO_SEM_RECARGA'
    ).show(10, truncate=False)

    # Schema final
    print("\n" + "="*100)
    print("SCHEMA DA VISÃO MENSAL")
    print("="*100 + "\n")

    recarga_mensal.printSchema()


def main():
    """
    Orquestrador principal - executa todas as etapas do processamento.
    """
    print("\n\n")
    print("╔" + "="*98 + "╗")
    print("║" + " "*98 + "║")
    print("║" + "PROCESSAMENTO DE RECARGA - TRATAMENTO E ENRIQUECIMENTO".center(98) + "║")
    print("║" + " "*98 + "║")
    print("╚" + "="*98 + "╝")
    
    # Etapa 1: Carregar e enriquecer base
    recarga = processar_recarga_base()
    
    # Etapa 2: Processar datas e tipos de valores
    recarga = processar_datas_e_tipos(recarga)
    
    # Etapa 3: Processar temporal
    recarga = processar_temporal(recarga)
    
    # Etapa 4: Agregar mensal
    recarga_mensal = agregar_recarga_mensal(recarga)
    
    # Etapa 5: Salvar em Delta e registrar tabelas
    salvar_delta_e_registrar_tabela(recarga, recarga_mensal)
    
    # Etapa 6: Validar e relatar
    validar_e_relatar(recarga, recarga_mensal)
    
    # Retornar dataframes para possível uso posterior
    return recarga, recarga_mensal


if __name__ == "__main__":
    recarga, recarga_mensal = main()
    
    print("\n\n")
    print("╔" + "="*98 + "╗")
    print("║" + "PROCESSAMENTO CONCLUÍDO COM SUCESSO!".center(98) + "║")
    print("╚" + "="*98 + "╝")
    print("\n")
