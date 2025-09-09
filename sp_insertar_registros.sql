DELIMITER //

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
END //

DELIMITER ;