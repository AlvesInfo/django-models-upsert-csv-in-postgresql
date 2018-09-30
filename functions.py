"""
Module apportant des fonctionnalités pratique à base de psycopg2
"""

import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.extensions import parse_dsn

TYPE_POSTGRESQL = {
    'bigint': ('int', 'validate_int'),
    'bigserial': ('int', 'validate_int'),
    'bit': ('bit', 'validate_bit'),
    'bit varying': ('bit', 'validate_bit'),
    'boolean': ('bool', 'validate_bool'),
    'box': ('box', 'validate_box'),
    'bytea': ('bit', 'validate_bit'),
    'character': ('str', 'validate_str'),
    'character varying': ('str', 'validate_str'),
    'cidr': ('cidr', 'validate_cidr'),
    'circle': ('circle', 'validate_circle'),
    'date': ('date', 'validate_date'),
    'double precision': ('float', 'validate_float'),
    'inet': ('inet', 'validate_inet'),
    'integer': ('int', 'validate_int'),
    'interval': ('interval', 'validate_interval'),
    'json': ('json', 'validate_json'),
    'jsonb': ('json', 'validate_json'),
    'line': ('line', 'validate_line'),
    'lseg': ('lseg', 'validate_lseg'),
    'macaddr': ('macaddr', 'validate_macaddr'),
    'macaddr8': ('macaddr8', 'validate_macaddr8'),
    'money': ('float', 'validate_float'),
    'numeric': ('float', 'validate_float'),
    'path': ('path', 'validate_path'),
    'pg_lsn': ('pg_lsn', 'validate_pg_lsn'),
    'point': ('point', 'validate_point'),
    'polygon': ('polygon', 'validate_polygon'),
    'real': ('real', 'validate_real'),
    'smallint': ('int', 'validate_int'),
    'smallserial': ('int', 'validate_int'),
    'serial': ('int', 'validate_int'),
    'text': ('str', 'validate_str'),
    'time': ('time', 'validate_time'),
    'time with time zone': ('time', 'validate_time'),
    'timestamp': ('datetime', 'validate_datetime'),
    'timestamp with time zone': ('datetime', 'validate_datetime'),
    'tsquery': ('tsquery', 'validate_tsquery'),
    'tsvector': ('tsvector', 'validate_tsvector'),
    'txid_snapshot': ('txid_snapshot', 'validate_txid_snapshot'),
    'uuid': ('uuid', 'validate_uuid'),
    'xml': ('xml', 'validate_xml'),
}


def clean_sql_in(sql, entier=None):
    """
    Fonction qui renvoie la fonction sql : in, proprement
        :param sql: [14, 28, ...] ou ['test', 'test2', ...]
        :param entier: Si la liste doit être des entiers
        :return: in (14, 28, ...) ou in ('test', 'test2', ...)
    """
    sql_in = ""

    if sql:
        if isinstance(sql, str):
            sql_r = str(sql).replace("'", "''")
            sql_in = f"IN ('{sql_r}')"

        else:
            sql_in = "IN ("

            for row in sql:
                sql_r = str(row).replace("'", "''")

                if entier:
                    sql_in += f"{sql_r}, "
                else:
                    sql_in += f"'{sql_r}', "

            sql_in = f"{sql_in[:-2]})"

    return sql_in
 

class WithCnxPostgresql:
    """
    Classe de connection à postgresql avec with
        conn = psycopg2.connect(dsn)

        # transaction 1
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)

        # transaction 2
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)

        conn.close()
    """
    def __init__(self, string_of_connexion):
        self.connexion = None
        i = 0
        while i < 5:
            try:
                kwargs_cnx = parse_dsn(string_of_connexion)
                self.connexion = psycopg2.connect(**kwargs_cnx)
                break

            except psycopg2.Error as error:
                log_line = f"WithCnxPostgresql error: {error}\n"
                print(log_line)

            i += 1

    def __enter__(self):
        return self.connexion

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connexion is not None:
            self.connexion.close()


def cnx_postgresql(string_of_connexion):
    """
    Fonction de connexion à Postgresql par psycopg2
        :param string_of_connexion: cnx_string = (
                                        f"dbname={NAME_DATABASE} "
                                        f"user={USER_DATABASE} "
                                        f"password={PASSWORD_DATABASE} "
                                        f"host={HOST_DATABASE} "
                                        f"port={PORT_DATABASE}"
                                    )
        :return: cnx
    """
    try:
        kwargs_cnx = parse_dsn(string_of_connexion)
        connexion = psycopg2.connect(**kwargs_cnx)

    except psycopg2.Error as error:
        log_line = f"cnx_postgresql error: {error}\n"
        print(log_line)
        connexion = None

    return connexion


def get_types_champs(cnx, table, list_champs=None):
    """
    Fonction qui récupère les types de champs de la table et des champs demandés pour la requête
        :return: list des champs, (taille, type, list des champs)
    """
    champs = f"AND column_name {clean_sql_in(list_champs)}" if list_champs is not None else ""

    with cnx.cursor() as cursor:
        sql_champs = f"""
            SELECT column_name, data_type, character_maximum_length, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = '{table}' 
            {champs}
        """
        # print(sql_champs)
        cursor.execute(sql_champs)
        list_champs_taille_type = {r[0]: tuple(r[1:]) for r in cursor.fetchall()}

    return list_champs_taille_type, list_champs


def execute_prepared_upsert(kwargs_upsert):
    """
    Fonction qui exécute une requete préparée, INSERT ou UPSERT.
    Attention!!! cette requête sera en autocommit.
    exemple :
    cursor.execute("PREPARE stmt (int, text, bool)
    AS INSERT INTO foo VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;")
    execute_batch(cursor, "EXECUTE stmt (%s, %s, %s)", list_values)
    cursor.execute("DEALLOCATE stmt")

        :param kwargs_upsert: dictionaire comprenant -->
                                      cnx: connexion psycopg2
                                    table: table concerné par la requête
                                   champs: champs de la table, souhaités dans la requête
                                     rows: Liste des valeurs à inserer dans la table
                            champs_unique: liste des champs d'unicité dans la table,
                                            si on veut un Upsert ON CONFLICT UPDATE
                                   upsert: None explicit, si on ne veut pas d'upsert
        :return: None
    """

    dict_rows = get_types_champs(
        kwargs_upsert['cnx'],
        kwargs_upsert['table'],
        kwargs_upsert['champs']
    )[0]
    prepare = "PREPARE stmt ("
    insert = "("
    colonnes = "("
    execute = "EXECUTE stmt ("

    for i, k in enumerate(kwargs_upsert['champs']):
        champ, t_p = k, dict_rows[k][0]
        prepare += f"{t_p}, "
        insert += f"${i + 1}, "
        colonnes += f'"{champ}", '
        execute += '%s, '

    colonnes = f'{colonnes[:-2]})'
    insert = f'{insert[:-2]})'
    prepare = f'''
    {prepare[:-2]}) AS INSERT INTO "{kwargs_upsert['table']}" {colonnes} VALUES {insert} 
    '''
    execute = f'{execute[:-2]});'

    if kwargs_upsert['upsert'] is None:
        prepare += ';'

    else:

        if kwargs_upsert['champs_unique'] is None:
            prepare += 'ON CONFLICT DO NOTHING;'

        else:
            chu = "("
            for k in kwargs_upsert['champs_unique']:
                chu += f'"{k}", '
            chu = f'{chu[:-2]})'
            prepare += f' ON CONFLICT {chu} DO UPDATE SET '
            for champ in kwargs_upsert['champs']:
                if champ not in kwargs_upsert['champs_unique']:
                    prepare += f'"{str(champ)}" = excluded."{str(champ)}", '

            prepare = f'{prepare[:-2]};'

    # print(prepare)
    # print(execute)
    with kwargs_upsert['cnx'] as cnx:
        with cnx.cursor() as cursor:
            cursor.execute(prepare)
            execute_batch(cursor, execute, kwargs_upsert['rows'])
            cursor.execute("DEALLOCATE stmt")


class GetModel:
    """
    Class de récupération des champs, des champs_et_type et des noms de table, pour un modèle
    dans Postgresql
    """

    def __init__(self, cnx, modele, **kwargs):
        """
        Initialisation de la class GetModel
            :param cnx: Connexion
            :param modele: Modèle
            :param kwargs: date_format=('-', 'Y', 'M', 'D')
                            exclude=None
                            fields=None
        """
        self.cnx = cnx
        self.modele = modele

        self.date_format = kwargs['date_format'] \
            if 'date_format' in kwargs else ('-', 'Y', 'M', 'D')
        self.exclude = kwargs['exclude'] if 'exclude' in kwargs else None
        self.fields = kwargs['fields'] if 'fields' in kwargs else None

    def get_model_fields(self):
        """
        Fonction qui retourne les champs de modèles
            :return: générateur de la liste des modèles
        """
        model = self.modele.__doc__.replace(self.modele.__name__, "").replace("(", "")
        model = model.replace(")", "").replace(" ", "")

        if self.exclude is not None:
            model = ','.join(r for r in model.split(",") if r not in self.exclude)
        elif self.fields is not None:
            model = ','.join(r for r in model.split(",") if r in self.fields)

        return (r for r in model.split(",") if r)

    def get_model_table_name(self):
        """
        Fonction qui retourne le nom de la table d'un modèle
        :return: le nom de la table dans postgresql
        """
        module = self.modele.__module__.split('.')[-2]
        table_name = f"{module}_{self.modele.__name__.lower()}"

        return table_name

    def get_champs_types(self):
        """
        Fonction de récupération des champs avec leur type pour les validations
            :return: genérateur des (champs, (type, taille, validateur))
                exemple:
                    [   ('num_facture', (30, True, 'validate_str')),                    <-- string
                        ('date_retour', (('-', 'Y', 'M', 'D'), True, 'validate_date')), <-- date
                        ('montant', (2, True, 'validate_float')),                       <-- float
                        ('qte_vte', (0, True, 'validate_int')),                         <-- int
                        ('test', (0, True, validate_bool) ]                             <-- booléen
        """
        champs = tuple(self.get_model_fields())
        table = self.get_model_table_name()
        champs_types = get_types_champs(self.cnx, table=table)[0]
        champs_validate = []

        for k in champs:
            sol = champs_types[k]
            tipe = TYPE_POSTGRESQL[sol[0]][0]
            l_g = self.date_format if tipe == 'date' else sol[1]
            bol = True if sol[2] == 'NO' else False
            validator = TYPE_POSTGRESQL[sol[0]][1]
            champs_validate.append((k, (l_g, bol, validator)))

        return table, champs_validate


LOG_FILE = os.path.join("/home", 'log_mise_a_jour.log')
LOG_FILE_DIVERS = os.path.join("/home", 'log_divers.log')


def write_log(fichier=None, line_to_write=None):
    """
    Fonction pour écrire une ligne de log dans un fichier
        :param fichier: Fichier ou écrire la ligne
        :param line_to_write: Ligne à écrire
        :return: None
    """
    if fichier and line_to_write:
        if not os.path.isfile(fichier):
            open(fichier, 'a').close()

        with open(fichier, 'a', encoding="utf-8") as log_file:
            log_file.write(line_to_write)


def envoi_mail_erreur(erreur, subject_error=None):
    """
    Fonction qui envoi les erreurs par mail
        :param erreur: Erreur à envoyer
        :param subject_error: Sujet de l'erreur
        :return: None
    """
    msg = MIMEMultipart()
    msg['From'] = EMAIL_HOST_USER
    msg['To'] = EMAIL_DEV

    if subject_error is None:
        msg['Subject'] = 'Erreur B.I'
    else:
        msg['Subject'] = subject_error
    print(erreur)
    message = erreur
    msg.attach(MIMEText(message))

    if EMAIL_USE_SSL:
        mailserver = smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT)
    else:
        mailserver = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)

    mailserver.ehlo()

    if EMAIL_USE_TLS:
        mailserver.starttls()

    mailserver.ehlo()
    mailserver.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
    mailserver.sendmail(EMAIL_HOST_USER, msg['To'], msg.as_string())
    mailserver.quit()


def delete_file(file):
    """
    Fonction qui supprime un fichier
        :param file: Chemin vers le fichier à supprimer
        :return: None
    """
    if os.path.isfile(file):
        os.remove(file)


def list_file(path, extension=None, reverse=None, first=None, name_part=None):
    """
    Fonction qui renvoie la liste des fichiers présent dans un répertoire
        :param path: Répertoire de recherche
        :param extension: Extension du fichier 'csv', 'xls', 'xlsx', 'txt' ....
        :param reverse: Tri de la liste, reverse=None -> ascendante et reverse=True -> descendante
        :param first: Si l'on veut tous les fichiers -> first=None, le premier fichier -> first=True
        :param name_part:   Partie d'un nom à rechercher -> name="TEST",
                            tous les fichiers  -> name=None
        :return: La liste des fichiers ou le fichier sinon None
    """
    path_name = f"{path}/*." if name_part is None else f"{path}/{str(name_part)}"

    if name_part is None:
        path_name = f"{path_name}*" if extension is None else f"{path_name}{extension}"

    list_files = glob.glob(path_name)

    if reverse is None:
        list_files.sort()
    else:
        list_files.sort(reverse=True)

    if first is None:
        return list_files if list_files else None

    return list_files[0] if list_files else None


class NotValidatorError(Exception):
    """
    Exception personalisée en cas ou un validateur n'existe pas
    """
    pass


def validate_bool(value, bool_type, col_name):
    """
    Fonction de validation des booléens
        :param value: valeur
        :param bool_type: type de bool
        :param col_name: nom de colonne
        :return: valeur validée
    """
    if not value:
        value_retour = '<NULL>'

    else:
        if value == 'f':
            value = False
        else:
            value = True

        value_retour = value

    # Il faut ajouter un test bool nullable pour vérification plus robuste
    print(bool_type, col_name)

    return value_retour


def validate_date(value, format_date, col_name):
    """
    Fonction de validation des dates
        :param value: valeur
        :param format_date: format de date demandé
        :param col_name: nom de colonne
        :return: valeur validée
    """
    if not value:
        value_retour = '<NULL>'
        return value_retour

    lg_valide = {'/', '-', '_', ':', 'D', 'M', 'Y', 'date_adp'}

    if any(r not in lg_valide for r in format_date):
        err = (f"Sep {', '.join(lg_valide)}, Obligatoires, "
               f"pour la colonne {col_name}\n")
        value_retour = (err,)

    elif format_date[0] not in value and format_date[0] != 'date_adp':
        err = (f"la séparation de date attendu, est {format_date[0]}, la date du fichier est : "
               f"{value}, pour la colonne {col_name}\n")
        value_retour = (err,)

    else:

        try:
            sep = format_date[0]

            if sep == 'date_adp':
                n_d = 0
                n_m = 1
                n_y = 2
                d_t = date_adp(value)

            else:
                d_t = value.split(' ')[0].split(sep)
                dte = format_date[1:]
                n_d = dte.index('D')
                n_m = dte.index('M')
                n_y = dte.index('Y')

            jour = int(d_t[n_d])
            mois = int(d_t[n_m])
            annee = int(d_t[n_y])

            if len(str(annee)) == 2:
                annee += 2000

            value_retour = date(annee, mois, jour)

        except (ValueError, IndexError):
            err = (f"la value '{value}' ne correspond pas à une date, pour la colonne "
                   f"{col_name}\n")
            value_retour = (err,)

    return value_retour


def validate_datetime(value):
    """
    Fonction de validation des datetime
        :param value: valeur
        :return: valeur validée
    """
    print(value)


def validate_float(value, decimale, col_name):
    """
    Fonction de validation des float
        :param value: valeur
        :param decimale: decimales
        :param col_name: nom de colonne
        :return: valeur validée
    """
    value = str(value).replace(',', '.').replace(' ', '')
    list_s = []

    for l_s in value[::-1]:

        try:
            int(l_s)
            list_s.append(l_s)

        except ValueError:
            if '.' not in list_s and l_s == '.' or '-' in l_s:
                list_s.append(l_s)
            elif '(' in l_s and '-' not in list_s:
                list_s.append('-')

    list_s.reverse()
    value = "".join(list_s)

    if not value:
        value_retour = int(0)

    elif decimale not in {0, 1}:
        err = (f"Pour un float, l_g doit être un int, 0 (pour int) ou 1 (pour float), "
               f"colonne {col_name}")
        value_retour = (err,)

    else:
        val = value
        try:
            v_a = float(val)
            if not value:
                v_a = int(0)
            elif v_a == 0:
                v_a = int(0)
            else:
                if decimale == 0:
                    v_a = int(v_a)
                elif decimale == 1:
                    v_a = float(v_a)

            value_retour = v_a

        except ValueError:
            err = f"la value '{val}' doit être un nombre, pour la colonne {col_name}\n"
            value_retour = (err,)

    return value_retour


def validate_int(value, decimale, col_name):
    """
    Fonction de validation des int
        :param value: valeur
        :param decimale: non nécessaire, juste pour la signature globle
        :param col_name: nom de colonne
        :return: valeur validée
    """
    value = str(value).replace(',', '.').replace(' ', '')
    list_s = []

    for l_s in value[::-1]:

        try:
            int(l_s)
            list_s.append(l_s)

        except ValueError:
            if '.' not in list_s and l_s == '.' or '-' in l_s:
                list_s.append(l_s)
            elif '(' in l_s and '-' not in list_s:
                list_s.append('-')

    list_s.reverse()
    value = "".join(list_s)

    if not value:
        value_retour = int(0)

    elif decimale not in {0, 1}:
        err = (f"Pour un float, l_g doit être un int, 0 (pour int) ou 1 (pour float), "
               f"colonne {col_name}")
        value_retour = (err,)

    else:
        val = value
        try:
            v_a = float(val)
            if not value:
                v_a = int(0)
            elif v_a == 0:
                v_a = int(0)
            else:
                if decimale == 0:
                    v_a = int(v_a)
                elif decimale == 1:
                    v_a = float(v_a)

            value_retour = v_a

        except ValueError:
            err = f"la value '{val}' doit être un nombre, pour la colonne {col_name}\n"
            value_retour = (err,)

    return value_retour


def validate_str(value, lg_str, col_name):
    """
    Fonction de validation des str
        :param value: valeur
        :param lg_str: longueur maxi du str à renvoyer
        :param col_name: nom de colonne
        :return: valeur validée
    """
    try:
        if not value:
            value = ''
        else:
            nb_car = int(lg_str)
            value = value.replace('"', '') \
                .replace("'", "''") \
                .replace('\n', '') \
                .replace('\r', '') \
                .replace('\t', '')
            value = value[:nb_car]

            if value == '0.0':
                value = '0'

        value_retour = value

    except ValueError:
        err = f"La longueur de str doit être un integer, colonne {col_name}\n"
        value_retour = (err,)

    return value_retour


def validate_text(value, lg_str, col_name):
    """
    Fonction de validation des texte
        :param value: valeur
        :param lg_str: on nécessaire, juste pour la signature globle
        :param col_name: nom de colonne
        :return: valeur validée
    """
    print(lg_str)

    try:
        if not value:
            value = ''
        else:
            value = value.replace('"', '') \
                .replace("'", "''") \
                .replace('\n', '') \
                .replace('\r', '') \
                .replace('\t', '')
            value = value[:2056]

            if value == '0.0':
                value = '0'

        value_retour = value

    except ValueError:
        err = f"La longueur de str doit être un integer, colonne {col_name}\n"
        value_retour = (err,)

    return value_retour


def clean_columns(clean_column):
    """
    fonction qui clean le nom des colonnes
        :param clean_column: colonne à cleaner
        :return: valeur cleanée
    """
    col = clean_column.strip().replace(' ', '_').replace('\n', '_').replace('\r', '_').lower()
    return col


def validate_element(value, col_name, tup_type):
    """
    Fonction de validation des données de leur type. La taille de la valeur pour les str
    est tronquée à la valeur demandée
        :param value: valeur a verifier
        :param col_name: nom ou numero de la colonne, pour information en cas d'erreur
        :param tup_type: le tuple de type de donnees
        :return: retourne la valeur nettoyee, ou l'erreur
    """
    l_g, mandatory, validator = tup_type
    valeur = str(value).strip()

    # Si l'on a pas de valeur dans la cellule et quelle est obligatoire, alors on lance une
    # erreur
    if not valeur and mandatory:
        err = f"une valeur est obligatoire, pour la colonne {col_name}, le champ est vide\n"
        valeur_retour = (err,)

    else:
        try:
            valeur_retour = globals()[validator](valeur, l_g, col_name)

        except KeyError:
            raise NotValidatorError(f"le validateur : {validator}, n'existe pas!'")

    return valeur_retour


def remove_columuns_lines(
        file_to_validate,
        csv_to_validate,
        columns_to_take,
        lines_to_delete=(),
        **csv_params
):
    """
    Fonction qui supprime les lignes et garde seulement les colonnes, demandées
        :param file_to_validate: Nom et chemin du fichier en entrée
        :param csv_to_validate: Nom et chemin du fichier en sortie
        :param columns_to_take: colonnes à conserver
        :param lines_to_delete: liste des lignes non souhaitées
        :param csv_params: parametres des fichiers csv : sep | encoding_e | encoding_s | errors
        :return: None
    """
    with open(
            file_to_validate,
            'r',
            encoding=csv_params['encoding_e'],
            errors=csv_params['errors'],
            newline=''
    ) as open_file:
        dialect = csv.Sniffer().sniff(open_file.readline())
        open_file.seek(0)
        reader = csv.reader(open_file, dialect, delimiter=csv_params['sep'])

        with open(
                csv_to_validate,
                'w',
                encoding=csv_params['encoding_s'],
                newline=''
        ) as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter=csv_params['sep'],
                quotechar='"',
                quoting=csv.QUOTE_NONNUMERIC
            )
            for row in iter_out_elements(reader, lines_to_delete):
                if ''.join(row).strip():
                    writer.writerow(iter_in_elements_order(row, columns_to_take))


def setting_delete_lines(del_lines, header_line):
    """
    Fonction qui applati les lignes à supprimer à la façon d'impression des pages
        ex:
            (1, 2, '4:7') --> {0, 1, 3, 4, 5, 6}
        :param del_lines: lignes à supprimer
        :param header_line: lignes entête à supprimer à supprimer
        :return: None en cas d'erreur de format envoyé, ou Set des lignes à supprimer
    """
    set_s = set()
    for line in del_lines:
        if isinstance(line, (int,)):
            set_s.add(line - 1)
        elif isinstance(line, (str,)):
            list_l = line.split(':')
            try:
                b_inf = int(list_l[0])
                b_sup = int(list_l[1]) + 1
                for n_b in range(b_inf, b_sup):
                    set_s.add(int(n_b) - 1)
            except ValueError:
                return None
        else:
            return None

    if header_line:
        set_s.add(header_line - 1)

    return set_s


def csv_file_validator(csv_file, list_columns=None):
    """
    Fonction de validation des fichiers
        :param csv_file: csv_file
        :param list_columns: list_columns
        :return: valeur validée
    """
    print(csv_file, list_columns)


class CsvTxtValidator:
    """
    Validation d'un fichier csv ou un txt avec un separateur. La fonction reçoit les colonnes et
    le format, que le fichier doit avoir. Si le fichier ne respecte pas la liste et format des
    colonnes, renvoie un tuple (None, log d'erreurs). Si le fichier respecte les directives,
    genère un csv sans entêtes.

        :param file_to_validate: le chemin complet du fichier
        :param columns_table: la liste des colonnes et leur format si necessaire.
                    True si la donnee est obligatoire
                    False si la donnee est optionelle
                    exemple:
                        [   ('num_facture', ('str', 30, True)),                     <-- string
                            ('date_retour', ('date', ('-', 'Y', 'M', 'D'), True)),  <-- date
                            ('montant', ('float', 1, True)),                        <-- float - 1
                            ('qte_vte', ('float', 0, True)),                        <-- int - 0
                            ('test', ('bool', 0, True) ]                            <-- booléen

        :param del_lines: Tuple des lignes à supprimer, commence à 1, on peut regrouper des
                            lignes consecutives. les regroupements consecutifs se feront dans un
                            str, avec : comme séparation exemple: (1, 2, '4:7')

        :param header_line: Si on a un header, on envoi le numéro de ligne du header, commence à 1
        :param desired_columns: tuple des noms ou numeros de colonnes, a prendre dans le fichier, si
                                columns_file est vide alors on conserve les colonnes du fchier et
                                dans le même ordre
                    exemple:
                        (2, 7, 10, 1)                                     <-- N° des colonnes, le
                                                                                meme nombre et ordre
                                                                                des colonnes
                                                                                (columns_table),
                                                                                commence à 1
                        ('col_f_c', 'col_f_n', 'col_f_na', 'col_f_jj')   <-- nom des colonnes
                                                                        fichier le meme nombre et
                                                                        ordre des colonnes
                                                                        (columns_table)
        :param sep: caractere de separation des lignes du fichier, par defaut -> ;
        :param sous_total_a_supprimer: un tuple de tuple des noms et position a supprimmer
                    exemple:
                        ((2, 'Sous Total'), (3, 'Total'))
        :param encoding_e: encoding du fichier reçu
        :param encoding_s: encoding du fichier traité
        :return: (header ou None), (nom du fichier validé ou lignes d'erreur)
    """
    TIME_SLEEP = 2

    # ==============================================================================================
    def __init__(self, file_to_validate, columns_table, error_dir, desired_columns=(), del_lines=(),
                 sous_total_a_supprimer=(), header_line=0, sep=";", encoding_e='utf-8',
                 encoding_s='utf-8', errors='replace'):
        self.file_to_validate = file_to_validate
        self.columns_table = columns_table
        self.error_dir = error_dir
        self.desired_columns = desired_columns
        self.del_lines = del_lines
        self.header_line = header_line
        self.sep = sep
        self.sous_total_a_supprimer = sous_total_a_supprimer
        self.encoding_e = encoding_e
        self.encoding_s = encoding_s
        self.errors = errors

    # ==============================================================================================
    def get_columns_position(self, col_fichier):
        """
        Fonction qui renvoie la position des colonnes demandées dans le fichier d'origine
            :param col_fichier: colonnes entêtes du fichier
            :return: (None, error) ou (True, list des nums de colonnes désirées)
        """
        list_desired_columns = [clean_columns(r) for r in self.desired_columns]
        set_desired_columns = set(list_desired_columns)

        list_file_columns = [clean_columns(r) for r in col_fichier]
        set_file_columns = set(list_file_columns)

        if set_file_columns.issuperset(set_desired_columns):
            # les colonnes y sont, alors on decouvre leur positions
            num_columns = []

            for column in list_desired_columns:
                num = int(list_file_columns.index(column))
                num_columns.append(num)

            return True, num_columns

        # Si elle n'y sont pas toutes alors on les listes, pour remonter l'erreur
        l_f = ", ".join(sorted(list_file_columns))
        l_c = ", ".join(sorted(list_desired_columns))

        error = (f"Le fichier  : {self.file_to_validate} \n\nNe contient pas toutes les "
                 f"colonnes demandées: \n\t"
                 f"colonnes du fichier : \n\t\t{l_f}\n\t"
                 f"colonnes demandées : \n\t\t{l_c}\n\n")

        return None, error

    # ==============================================================================================
    @property
    def validation(self):
        """
        Validation du fichier, reçu en paramètre
            :return: (None, Erreur) ou (True, fichier)
        """
        base_dir = os.path.dirname(self.file_to_validate)
        base_name = os.path.basename(self.file_to_validate)
        file_name_error = "ERRORS_" + base_name
        csv_file_to_validate_error = os.path.join(self.error_dir, file_name_error)
        delete_file(csv_file_to_validate_error)

        # On vérifie si le fichier existe
        if not os.path.isfile(self.file_to_validate):
            error = f"Le fichier demandé : {self.file_to_validate}\n\tn'existe pas!\n"
            return None, error

        # On vérifie si l'extension est bien .csv ou .txt. Si ce n'est pas le cas on retourne
        # une erreur
        ext = os.path.splitext(os.path.basename(self.file_to_validate))[-1]
        fichier = os.path.basename(self.file_to_validate)

        if str(ext) not in {'.csv', '.txt'}:
            error = f"Le fichier doit ête un csv ou un txt : {fichier}\n"
            move_file(self.file_to_validate, csv_file_to_validate_error)
            return None, error

        # On vérifie si self.del_lines est conforme au format attendu
        set_delete_lines = setting_delete_lines(self.del_lines, self.header_line)

        if set_delete_lines is None:
            error = (f"Il y a une erreur dans les lignes à supprimer : {self.del_lines}\n\t"
                     f"elle doivent être de type (0, 2, '4:7')\n")
            move_file(self.file_to_validate, csv_file_to_validate_error)
            return None, error

        nb_delele_lines = len(set_delete_lines)

        # On vérifie si les colonnes demandées sont dans le fichier
        table_columns = [r[0] for r in self.columns_table]

        if self.header_line:
            num_line = self.header_line - 1
        else:
            num_line = 0

        list_col_file = []

        # lecture des entêtes du fichier
        with open(self.file_to_validate, 'r', encoding=self.encoding_e, errors=self.errors,
                  newline='') as open_file:
            dialect = csv.Sniffer().sniff(open_file.readline())
            open_file.seek(0)
            reader = csv.reader(open_file, dialect, delimiter=self.sep)

            for k, ligne in enumerate(reader):
                if k == num_line:
                    list_col_file = [
                        r.strip().replace(' ', '_').replace('\n', '_').replace('\r', '_').lower()
                        for r in ligne]
                    break

            nb_columns_file = len(list_col_file)

        # Vérification des colonnes
        if self.desired_columns:
            if not isinstance(self.desired_columns, (list, tuple, set)):
                error = "Les colonnes souhaitées doivent être au format : list, tuple ou set\n"
                move_file(self.file_to_validate, csv_file_to_validate_error)
                return None, error

            if isinstance(self.desired_columns[0], int):
                max_num_column = max(self.desired_columns)

                if max_num_column > nb_columns_file:
                    error = f"Le fichier doit avoir au moins {max_num_column}, colonnes\n"
                    move_file(self.file_to_validate, csv_file_to_validate_error)
                    return None, error

                desired_columns = self.desired_columns

            else:
                test, desired_columns = self.get_columns_position(list_col_file)

                if test is None:
                    error = desired_columns
                    move_file(self.file_to_validate, csv_file_to_validate_error)
                    return None, error

            nb_columns_desired_columns = len(set(desired_columns))

            if nb_columns_desired_columns > nb_columns_file:
                error = f"Le fichier doit avoir au moins {nb_columns_desired_columns}, colonnes\n"
                move_file(self.file_to_validate, csv_file_to_validate_error)
                return None, error

            columns = desired_columns

        else:
            nb_columns_table = len(self.columns_table)

            if nb_columns_table > nb_columns_file:
                error = f"Le fichier doit avoir au moins {nb_columns_table}, colonnes\n"
                move_file(self.file_to_validate, csv_file_to_validate_error)
                return None, error

            columns = [k for k in range(nb_columns_table)]

        # Contrôle des types, de toutes les lignes conformes aux colonnes de la table
        file_name_to_validate = "TO_VALIDATED_" + base_name
        csv_to_validate = os.path.join(base_dir, file_name_to_validate)

        csv_params = {
            'sep': self.sep,
            'encoding_e': self.encoding_e,
            'encoding_s': self.encoding_s,
            'errors': self.errors
        }

        remove_columuns_lines(
            self.file_to_validate,
            csv_to_validate,
            columns,
            set_delete_lines,
            **csv_params
        )

        time.sleep(CsvTxtValidator.TIME_SLEEP)
        file_name_validated = "VALIDATED_" + base_name
        csv_file_validated = os.path.join(base_dir, file_name_validated)

        list_errors = []
        error = None

        with open(
                csv_to_validate,
                'r',
                encoding=self.encoding_s,
                errors=self.errors,
                newline=''
        ) as open_file:
            reader = csv.reader(open_file, delimiter=self.sep)

            with open(csv_file_validated, 'w', encoding=self.encoding_s, newline='') as csvfile:
                csv_write = csv.writer(
                    csvfile,
                    delimiter=self.sep,
                    quotechar='"',
                    quoting=csv.QUOTE_NONNUMERIC
                )

                # On vérifie toutes les colonnes. Si l'on trouve une erreur, alors on parcours
                # le fichier, pour remonter les 50 premières erreurs et les loguées
                n_ligne = 1 + nb_delele_lines
                err = 0
                for lig in reader:
                    errors = []
                    ligne = []

                    for i, row in enumerate(self.columns_table):
                        col, tup = row
                        val = validate_element(lig[i], col, tup)

                        if isinstance(val, tuple):
                            if not errors:
                                errors.append(n_ligne)
                            position = columns[i] + 1
                            val = val[0] + f" -- en position {position}"
                            errors.append(val)
                            error = True

                        if not list_errors and not errors:
                            ligne.append(val)

                    if errors:
                        err += 1
                        list_errors.append(errors)
                        if len(list_errors) >= 50:
                            break

                    if not list_errors:
                        csv_write.writerow(ligne)

                    n_ligne += 1

        time.sleep(CsvTxtValidator.TIME_SLEEP)

        # Si il y a des erreurs on les renvoient
        if error:
            log_error = f"""Erreurs repérées dans le fichier {fichier}\n"""

            for row in list_errors:
                ligne_en_erreur = row[0] + nb_delele_lines
                erreurs_de_la_ligne = row[1:]
                log_error += f"    * ligne {ligne_en_erreur} :\n"

                for erreur_ligne in erreurs_de_la_ligne:
                    log_error += f"            - {erreur_ligne}\n"
            move_file(self.file_to_validate, csv_file_to_validate_error)
            delete_file(csv_to_validate)
            delete_file(csv_file_validated)

            return None, log_error

        delete_file(csv_to_validate)
        delete_file(self.file_to_validate)

        return table_columns, csv_file_validated
