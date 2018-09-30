"""
Module générique d'intégration de fichier sur un modèle
"""
import sys
import csv
from datetime import datetime as dt
import time

from functions import (
    cnx_postgresql,
    execute_prepared_upsert,
    GetModel,
    delete_file,
    list_file,
    CsvTxtValidator
)

TIME_SLEEP = 2


def integration_file_csv(kwargs_cnx, kwargs_file, kwargs_modele, kwargs_validate, kwargs_upsert):
    """
    Intégration génerique de fichiers en base de données
              :param kwargs_cnx: Paramètres pour string_connection
                                    kwargs_cnx = {
                                        NAME_DATABASE,
                                        USER_DATABASE,
                                        PASSWORD_DATABASE,
                                        HOST_DATABASE,
                                        PORT_DATABASE
                                    }
             :param kwargs_file: Paramètres pour list_file(**kwargs_file)
                                    kwargs_file = {
                                        path,
                                        extension=None,
                                        reverse=None,
                                        first=None,
                                        name_part=None
                                    }
           :param kwargs_modele: Paramètres pour GetModel(cnx, **kwargs_modele)
                                    kwargs_modele = {
                                        modele,
                                        date_format=('-', 'Y', 'M', 'D'),
                                        exclude=None ou fields=None,
                                    }
        :param kwargs_validate: Paramètres pour CsvTxtValidator(file_csv, champs, **kwargs_validate)
                                    kwargs_validate = {
                                        error_dir,
                                        desired_columns=(),
                                        del_lines=(),
                                        sous_total_a_supprimer=(),
                                        header_line=0,
                                        sep=";",
                                        encoding_e='utf-8',
                                        encoding_s='utf-8',
                                        errors='replace'
                                    }
         :param kwargs_upsert: Paramètres pour execute_prepared_upsert(kwargs_upsert)
                                    kwargs_upsert = {
                                        champs_unique=('test', ),
                                        upsert=True
                                    }
        :return: None ou True, "success"
    """
    csv_valid = ""

    try:
        # On se connecte à postgresql
        cnx_string = (
            f"dbname={kwargs_cnx['NAME_DATABASE']} "
            f"user={kwargs_cnx['USER_DATABASE']} "
            f"password={kwargs_cnx['PASSWORD_DATABASE']} "
            f"host={kwargs_cnx['HOST_DATABASE']} "
            f"port={kwargs_cnx['PORT_DATABASE']}"
        )

        postgres_cnx = cnx_postgresql(cnx_string)

        # On verifie si on a la connexion à postgresql
        if postgres_cnx is None:
            log_line = (
                f'{dt.now().isoformat()} | integration_file_csv : pas de connexion à postgresql\n'
            )
            envoi_mail_erreur(log_line)
            write_log(LOG_FILE, log_line)
            return None, log_line

        # On récupère le fichier
        file_csv = list_file(**kwargs_file)[0]

        if file_csv is None:
            log_line = (
                f'{dt.now().isoformat()} | integration_file_csv : '
                f'pas de fichier {file_csv} à mettre à jour\n'
            )
            envoi_mail_erreur(log_line)
            write_log(LOG_FILE, log_line)
            return None, log_line

        # Lancement validation du csv
        model_def = GetModel(postgres_cnx, **kwargs_modele)
        table, champs_type = model_def.get_champs_types()
        colonnes, csv_valid = CsvTxtValidator(
            file_csv,
            champs_type,
            **kwargs_validate
        ).validation

        # On verifie si le fichier n'est pas valide
        if colonnes is None:
            envoi_mail_erreur(csv_valid)
            log_line = csv_valid
            write_log(LOG_FILE, log_line)
            return None, log_line

        champs = [r[0] for r in champs_type]

        # On lance la mise à jour depuis le csv vérifié
        with open(csv_valid, newline='', encoding='utf-8', errors='replace') as csvfile:
            file_reader = csv.reader(csvfile, delimiter=';')
            kwargs_upsert['cnx'] = postgres_cnx
            kwargs_upsert['table'] = table
            kwargs_upsert['champs'] = champs
            kwargs_upsert['rows'] = file_reader
            execute_prepared_upsert(kwargs_upsert)

        ligne = (
            f'{dt.now().isoformat()} | integration_file_csv : le modèle '
            f'{kwargs_modele["modele"].__name__} '
            f'a été mis à jour\n'
        )
        write_log(LOG_FILE, ligne)

    except:
        ligne = f'{dt.now().isoformat()} | integration_file_csv : ' \
                f'{csv_valid}\n\t\t{sys.exc_info()[1]}\n'
        write_log(LOG_FILE, ligne)
        envoi_mail_erreur(ligne)

    finally:
        delete_file(csv_valid)
        time.sleep(TIME_SLEEP)

    return True, "success"
