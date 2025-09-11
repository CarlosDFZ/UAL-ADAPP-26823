import mysql.connector
from rapidfuzz import process, fuzz

def conectar_bd(database):
    try:
        conexion = mysql.connector.connect(
            host="localhost",
            user="root",
            password="", 
            database=database
        )
        return conexion
    except mysql.connector.Error as error:
        print(f"Error al conectar a MySQL: {error}")
        return None
    
def fuzzy_match(queryRecord, choices, score_cutoff=0):
    scorers = [fuzz.WRatio, fuzz.QRatio, fuzz.token_set_ratio, fuzz.ratio]
    processor = lambda x: str(x).lower()
    processed_query = processor(queryRecord)
    choices_data = []

    for choice in choices:
        dict_choices = dict(choice)
        queryMatch = ""
        dict_match_records = {}
        for k, v in dict_choices.items():
            if k != "DestRecordId":
                val = str(v) if v is not None else ""
                queryMatch += val
                dict_match_records[k] = v

        choices_data.append({
            'query_match': queryMatch,
            'dest_record_id': dict_choices.get('DestRecordId'),
            'match_record_values': dict_match_records
        })

    best_match = None
    best_score = score_cutoff

    for scorer in scorers:
        result = process.extractOne(
            query=processed_query,
            choices=[item['query_match'] for item in choices_data],
            scorer=scorer,
            score_cutoff=score_cutoff,
            processor=processor
        )

        if result:
            match_value, score, index = result
            if score >= best_score:
                matched_item = choices_data[index]
                best_match = {
                    'match_query': queryRecord,
                    'match_result': match_value,
                    'score': score,
                    'match_result_values': matched_item['match_record_values']
                }
        else:
            best_match = {
                'match_query': queryRecord,
                'match_result': None,
                'score': 0,
                'match_result_values': {}
            }
    return best_match


def execute_dynamic_matching(params_dict, score_cutoff=0):
    conn = conectar_bd(
        database=params_dict.get("database", "")
    )
    cursor = conn.cursor()

    if 'src_dest_mappings' not in params_dict or not params_dict['src_dest_mappings']:
        raise ValueError("Debe proporcionar src_dest_mappings con columnas origen y destino")

    src_cols = ", ".join(params_dict['src_dest_mappings'].keys())
    dest_cols = ", ".join(params_dict['src_dest_mappings'].values())

    sql_source = f"SELECT {src_cols} FROM {params_dict['sourceSchema']}.{params_dict['sourceTable']}"
    sql_dest   = f"SELECT {dest_cols} FROM {params_dict['destSchema']}.{params_dict['destTable']}"

    cursor.execute(sql_source)
    src_rows = cursor.fetchall()
    src_columns = [col[0] for col in cursor.description]
    source_data = [dict(zip(src_columns, row)) for row in src_rows]

    cursor.execute(sql_dest)
    dest_rows = cursor.fetchall()
    dest_columns = [col[0] for col in cursor.description]
    dest_data = [dict(zip(dest_columns, row)) for row in dest_rows]

    conn.close()

    matching_records = []

    for record in source_data:
        dict_query_records = {}
        query = ""

        for src_col in params_dict['src_dest_mappings'].keys():
            val = record.get(src_col)
            query += str(val) if val is not None else ""
            dict_query_records[src_col] = val

        fm = fuzzy_match(query, dest_data, score_cutoff)
        dict_query_records.update(fm)
        dict_query_records.update({
            'destTable': params_dict['destTable'],
            'sourceTable': params_dict['sourceTable']
        })
        matching_records.append(dict_query_records)

    return matching_records

def fuzzy_match_weighted(queryRecord, choices, score_cutoff=0, column_weights=None):

    if not column_weights:
        # Pesos por defecto
        column_weights = {'first_name': 2, 'last_name': 3, 'email': 5}
    
    # Procesar los datos
    choices_data = []
    for choice in choices:
        dict_choices = dict(choice)
        dict_match_records = {}
        for k, v in dict_choices.items():
            if k != "DestRecordId":
                dict_match_records[k] = v if v is not None else ""
        
        choices_data.append({
            'dest_record_id': dict_choices.get('DestRecordId'),
            'match_record_values': dict_match_records
        })
    
    best_match = None
    best_score = score_cutoff
    
    # Calcular la suma total de pesos
    total_weight = sum(column_weights.values())
    
    for choice_data in choices_data:
        choice_values = choice_data['match_record_values']
        weighted_scores = []
        
        # Calcular puntuación
        for column, weight in column_weights.items():
            # Obtener valores para comparar
            query_value = str(queryRecord.get(column, "")).lower().strip()
            choice_value = str(choice_values.get(column, "")).lower().strip()
            
            # Si ambos valores están vacíos, asignar puntuación neutra
            if not query_value and not choice_value:
                column_score = 50.0  # Puntuación neutra
            elif not query_value or not choice_value:
                column_score = 0.0   # Penalizar valores faltantes
            else:
                column_score = fuzz.ratio(query_value, choice_value)
            
            weighted_score = (column_score * weight) / 100.0
            weighted_scores.append(weighted_score)
        
        # Puntuación final
        final_score = (sum(weighted_scores) / total_weight) * 100.0
        
        # Verificar coincidencia
        if final_score >= best_score:
            best_score = final_score
            
            # Cadena para compatibilidad
            query_match = ""
            for column in column_weights.keys():
                val = queryRecord.get(column, "")
                query_match += str(val) if val is not None else ""
            
            # Crear cadena de resultado
            match_result = ""
            for column in column_weights.keys():
                val = choice_values.get(column, "")
                match_result += str(val) if val is not None else ""
            
            best_match = {
                'match_query': query_match,
                'match_result': match_result,
                'score': round(final_score, 2),
                'match_result_values': choice_values,
                'column_scores': {
                    column: round(fuzz.ratio(
                        str(queryRecord.get(column, "")).lower().strip(),
                        str(choice_values.get(column, "")).lower().strip()
                    ), 2) for column in column_weights.keys()
                }
            }
    
    # Si no se encontró ninguna coincidencia, devolver estructura vacía
    if best_match is None:
        query_match = ""
        for column in column_weights.keys():
            val = queryRecord.get(column, "")
            query_match += str(val) if val is not None else ""
        
        best_match = {
            'match_query': query_match,
            'match_result': None,
            'score': 0,
            'match_result_values': {},
            'column_scores': {}
        }
    
    return best_match


def execute_dynamic_matching_weighted(params_dict, score_cutoff=0, column_weights=None):
    
    conn = conectar_bd(database=params_dict.get("database", ""))
    
    if not conn:
        return []
    
    cursor = conn.cursor()

    if 'src_dest_mappings' not in params_dict or not params_dict['src_dest_mappings']:
        raise ValueError("Debe proporcionar src_dest_mappings con columnas origen y destino")

    src_cols = ", ".join(params_dict['src_dest_mappings'].keys())
    dest_cols = ", ".join(params_dict['src_dest_mappings'].values())

    sql_source = f"SELECT {src_cols} FROM {params_dict['sourceSchema']}.{params_dict['sourceTable']}"
    sql_dest = f"SELECT {dest_cols} FROM {params_dict['destSchema']}.{params_dict['destTable']}"

    cursor.execute(sql_source)
    src_rows = cursor.fetchall()
    src_columns = [col[0] for col in cursor.description]
    source_data = [dict(zip(src_columns, row)) for row in src_rows]

    cursor.execute(sql_dest)
    dest_rows = cursor.fetchall()
    dest_columns = [col[0] for col in cursor.description]
    dest_data = [dict(zip(dest_columns, row)) for row in dest_rows]

    conn.close()

    # Configurar pesos
    if column_weights is None:
        column_weights = {'first_name': 2, 'last_name': 3, 'email': 5}
    
    # Mapear los peso
    mapped_weights = {}
    for src_col, dest_col in params_dict['src_dest_mappings'].items():
        if src_col in column_weights:
            mapped_weights[src_col] = column_weights[src_col]
        elif dest_col in column_weights:
            mapped_weights[src_col] = column_weights[dest_col]

    matching_records = []

    for record in source_data:
        # Preparar registro con las columnas mapeadas
        query_record = {}
        for src_col in params_dict['src_dest_mappings'].keys():
            query_record[src_col] = record.get(src_col)

        # Preparar datos de destino ya mapeado
        mapped_dest_data = []
        for dest_record in dest_data:
            mapped_record = {'DestRecordId': dest_record.get('cliente_id', '')}
            for src_col, dest_col in params_dict['src_dest_mappings'].items():
                mapped_record[src_col] = dest_record.get(dest_col)
            mapped_dest_data.append(mapped_record)

        # Realizar match ponderado
        fm = fuzzy_match_weighted(query_record, mapped_dest_data, score_cutoff, mapped_weights)
        
        # Agregar información
        result_record = record.copy()
        result_record.update(fm)
        result_record.update({
            'destTable': params_dict['destTable'],
            'sourceTable': params_dict['sourceTable']
        })
        
        matching_records.append(result_record)

    return matching_records