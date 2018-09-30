# django-models-upsert-csv-in-postgresql
"""
Intégration génerique de fichiers csv en base de données pour un modèle Django
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
