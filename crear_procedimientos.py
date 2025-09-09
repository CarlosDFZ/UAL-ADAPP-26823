import mysql.connector

def crear_procedimientos():
    procedimientos = {
        'sp_eliminar_tabla': """
            CREATE PROCEDURE sp_eliminar_tabla(IN p_tabla_nombre VARCHAR(255))
            BEGIN
                SET @sql = CONCAT('DROP TABLE IF EXISTS ', p_tabla_nombre);
                PREPARE stmt FROM @sql;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
            END
        """,
        
        'sp_crear_tabla': """
            CREATE PROCEDURE sp_crear_tabla(
                IN p_tabla_nombre VARCHAR(255),
                IN p_columnas_def TEXT
            )
            BEGIN
                SET @sql = CONCAT('CREATE TABLE ', p_tabla_nombre, ' (', p_columnas_def, ')');
                PREPARE stmt FROM @sql;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
            END
        """,
        
        'sp_insertar_registros': """
            CREATE PROCEDURE sp_insertar_registros(
                IN p_tabla_nombre VARCHAR(255),
                IN p_columnas_str TEXT,
                IN p_valores_sql TEXT
            )
            BEGIN
                SET @sql = CONCAT('INSERT INTO ', p_tabla_nombre, ' (', p_columnas_str, ') VALUES ', p_valores_sql);
                PREPARE stmt FROM @sql;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
            END
        """
    }
    
    bases_datos = ['dbo', 'crm']
    
    for base in bases_datos:
        print(f"Creando procedimientos en {base}...")
        try:
            conn = mysql.connector.connect(
                host="localhost",
                user="root",
                password="",
                database=base
            )
            cursor = conn.cursor()
            
            # Primero eliminar los procedimientos si existen
            for nombre in procedimientos.keys():
                try:
                    cursor.execute(f"DROP PROCEDURE IF EXISTS {nombre}")
                except:
                    pass  # Ignorar errores si no existe
            
            # Crear cada procedimiento
            for nombre, sql in procedimientos.items():
                try:
                    cursor.execute(sql)
                    print(f"  ✓ {nombre} creado")
                except mysql.connector.Error as e:
                    print(f"  ✗ Error en {nombre}: {e}")
                    print(f"  SQL: {sql}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except mysql.connector.Error as e:
            print(f"✗ Error conectando a {base}: {e}")

if __name__ == "__main__":
    crear_procedimientos()