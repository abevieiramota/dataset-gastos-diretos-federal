# -*- coding: utf-8 -*-
import os
import click
import logging
import pandas as pd
import zipfile
import re
from sklearn.preprocessing import LabelEncoder

def STRIP(text):

    try:
        return text.strip()
    except AttributeError:
        return text

class GastosDiretosExtractor:

    COLUMNS_RENAME = {
        'Nome Órgao': 'orgao', 
        'Nome Elemento Despesa': 'elemento_despesa', 
        'Nome Função': 'funcao', 
        'Nome Subfunção': 'subfuncao',
        'Nome Programa': 'programa', 
        'Nome Ação': 'acao', 
        'Código Favorecido': 'cod_favorecido', 
        'Nome Favorecido': 'nome_favorecido', 
        'Data Pagamento': 'data', 
        'Valor': 'valor'
    }

    READ_CFG = {
        "sep": "\t",
        "encoding": "iso-8859-1", 
        "decimal": ",",
        "usecols": COLUMNS_RENAME.keys(),
        "iterator": True,
        "chunksize": 10000,
        "converters": {column_name:STRIP for column_name in COLUMNS_RENAME.keys()}
    }

    CAT_COLS = ['Nome Órgao',
                'Nome Elemento Despesa',
                'Nome Função',
                'Nome Subfunção',
                'Nome Programa',
                'Nome Ação',
                'Código Favorecido',
                'Nome Favorecido']

    CAT_ID_COLS = [colname + "-ID" for colname in CAT_COLS]

    EXTRACT_ANOMES = re.compile('\d{6}')


    def __init__(self, include_filter):

        self.logger = logging.getLogger("GastosDiretosExtractor")
        
        self.include_filter = include_filter
        self.files = [filename for filename in os.listdir(".") if filename.endswith(".zip")]

        self.df = None
        self.encoders = {}


    def filter_in(self, df):

        return df[self.include_filter(df)]


    def ler_zip_csv(self, filepath):

        self.logger.info('>Processando arquivo {}'.format(filepath))
        
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            
            # TODO: assume que há apenas um arquivo
            filename =zip_ref.namelist()[0]
                
            with zip_ref.open(filename) as zip_file:
        
                #http://stackoverflow.com/questions/13651117/pandas-filter-lines-on-load-in-read-csv
                iter_csv = pd.read_csv(zip_file, **GastosDiretosExtractor.READ_CFG)

                df = pd.concat([self.filter_in(chunk) for chunk in iter_csv])

        return df

    def extract_data(self):

        self.logger.info("EXTRAINDO OS DADOS")

        dfs = [self.ler_zip_csv(zip_file) for zip_file in self.files]

        self.df = pd.concat(dfs, ignore_index=True).fillna("NÃO-ESPECIFICADO")

    def make_encoders(self):

        for (colname, id_colname) in zip(GastosDiretosExtractor.CAT_COLS, GastosDiretosExtractor.CAT_ID_COLS):

            self.logger.info(">Normalizando coluna {}".format(colname))

            encoder = LabelEncoder().fit(self.df[colname].unique())
        
            encoder_df = pd.DataFrame(encoder.classes_, columns=[colname])
            encoder_df.index.name = id_colname
            encoder_df.to_csv(colname + ".csv", encoding='utf-8')
            
            self.encoders[colname] = encoder

        

    def normalize(self):

        self.logger.info("NORMALIZANDO COLUNAS")

        self.make_encoders()

        self.df[GastosDiretosExtractor.CAT_ID_COLS] = self.df[GastosDiretosExtractor.CAT_COLS]\
        .apply(lambda col: self.encoders[col.name].transform(col))
        self.df.drop(GastosDiretosExtractor.CAT_COLS, axis=1, inplace=True)


    def rename_columns(self):

    	self.df.rename(columns=GastosDiretosExtractor.COLUMNS_RENAME, inplace=True)


    def save_df(self):

        self.logger.info("SALVANDO O DATASET FINAL")

        self.df.to_csv("dataset.csv", index=False, encoding='utf-8')


    def processar(self):

        self.logger.info('CRIANDO DATASET FINAL')

        self.extract_data()
        self.normalize()
        self.rename_columns()
        self.save_df()


if __name__ == "__main__":

    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    only_UFC = lambda df: df["Nome Órgao"] == "UNIVERSIDADE FEDERAL DO CEARA"

    gde = GastosDiretosExtractor(include_filter = only_UFC)

    gde.processar()