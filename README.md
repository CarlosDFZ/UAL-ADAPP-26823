connect_to_azure_sql(): Establece una conexion con una base de datos SQL Server de Azure utilizando el driver ODBC.

Parametros:
    server: Nombre o dirección IP del servidor SQL
    database: Nombre de la base de datos
    username: Nombre de usuario para la autenticacion.
    password: Contraseña para la autenticacion.

Retorno:
    Connection: Objeto de conexion a la base de datos.


fuzzy_match(): Realiza una coincidencia difusa entre un registro de consulta y una lista de opciones.

Parametros: 
    queryRecord: Registro a buscar.
    choices: Lista de registros entre los que buscar coincidencias.
    score_cutoff: Puntuación mínima para considerar una coincidencia (default=0).

Algoritmos utilizados:
    fuzz.WRatio: Ratio ponderado.
    fuzz.QRatio: Ratio rápido.
    fuzz.token_set_ratio: Ratio de conjunto de tokens.
    fuzz.ratio: Ratio simple.

Retorno:
    Diccionario con:
        match_query: Consulta original.
        match_result: Mejor coincidencia encontrada.
        score: Puntuación de la coincidencia.
        match_result_values: Valores del registro coincidente.


execute_dynamic_matching(): Ejecuta una coincidencia dinámica entre dos tablas de base de datos.

Parametros:
    params_dict: Diccionario con la configuracion =
        {"server": str,
        "database": str...}
    score_cutoff: Puntuación mínima para coincidencias (default=70).
        Se configuró para mostrar solo coincidencias con más del 70% de similitud.

Retorno:
    Lista de diccionarios con las coincidencias encontradas que superen el umbral de 70%.

Notas de optimización:
- Se implementó inserción por lotes en las operaciones INSERT
- Se optimizó el manejo de las conexiones usando context managers
- El procesamiento de datos se realiza en bloques de 100 registros para un mejor uso de memoria