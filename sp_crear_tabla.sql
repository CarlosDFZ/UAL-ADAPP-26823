DELIMITER //

CREATE PROCEDURE sp_crear_tabla(
    IN p_tabla_nombre VARCHAR(255),
    IN p_columnas_def TEXT
)
BEGIN
    SET @sql = CONCAT('CREATE TABLE ', p_tabla_nombre, ' (', p_columnas_def, ')');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;