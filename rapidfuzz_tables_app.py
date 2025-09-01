from fuzz_functions import execute_dynamic_matching


params_dict = {
    "server": "localhost",
    "database": "dbo",
    "username": "root",
    "password": "",
    "sourceSchema": "dbo",
    "sourceTable": "Usuarios",
    "destSchema": "crm",
    "destTable": "Clientes",
    "src_dest_mappings": {
        "first_name": "nombre",
        "last_name": "apellido",
        "email" : "email"
    }
}

# Se cambió el score_cutoff a 70
resultados = execute_dynamic_matching(params_dict, score_cutoff=70)
print(resultados)
