DELIMITER //

CREATE PROCEDURE sp_insertClientesFromCSV_crm_26823(
    IN p_cliente_id VARCHAR(255),
    IN p_nombre VARCHAR(255),
    IN p_apellido VARCHAR(255),
    IN p_email VARCHAR(255),
    IN p_FechaRegistro DATETIME
)
BEGIN
    INSERT INTO Clientes 
    (cliente_id, nombre, apellido, email, FechaRegistro)
    VALUES 
    (p_cliente_id, p_nombre, p_apellido, p_email, p_FechaRegistro);
END //

DELIMITER ;