DELIMITER //

CREATE PROCEDURE sp_eliminar_tabla(IN p_tabla_nombre VARCHAR(255))
BEGIN
    SET @sql = CONCAT('DROP TABLE IF EXISTS ', p_tabla_nombre);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;