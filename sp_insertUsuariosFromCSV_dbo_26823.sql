DELIMITER //

CREATE PROCEDURE sp_insertUsuariosFromCSV_dbo_26823(
    IN p_userId VARCHAR(255),
    IN p_username VARCHAR(255),
    IN p_first_name VARCHAR(255),
    IN p_last_name VARCHAR(255),
    IN p_email VARCHAR(255),
    IN p_password_hash VARCHAR(255),
    IN p_rol VARCHAR(255),
    IN p_fecha_creacion DATETIME
)
BEGIN
    INSERT INTO Usuarios 
    (userId, username, first_name, last_name, email, password_hash, rol, fecha_creacion)
    VALUES 
    (p_userId, p_username, p_first_name, p_last_name, p_email, p_password_hash, p_rol, p_fecha_creacion);
END //

DELIMITER ;