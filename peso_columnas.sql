USE dbo;

-- Crear tabla para almacenar pesos de columnas
CREATE TABLE IF NOT EXISTS peso_columnas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    column_name VARCHAR(50) NOT NULL UNIQUE,
    weight DECIMAL(5,2) NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    updated_by VARCHAR(50) NOT NULL,
    CONSTRAINT chk_weight CHECK (weight >= 0)
) ENGINE=InnoDB;

-- Insertar valores iniciales
INSERT INTO peso_columnas (column_name, weight, updated_by) VALUES
('first_name', 2, 'system'),
('last_name', 3, 'system'),
('email', 5, 'system')
ON DUPLICATE KEY UPDATE
    weight = VALUES(weight),
    updated_by = VALUES(updated_by);

-- Stored Procedure para actualizar pesos
DELIMITER //

CREATE PROCEDURE UpdateColumnWeight(
    IN p_column_name VARCHAR(50),
    IN p_weight DECIMAL(5,2),
    IN p_updated_by VARCHAR(50)
)
BEGIN

    -- Validar el peso
    IF p_weight < 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'El peso debe ser un valor positivo';
    END IF;

    -- Actualizar el peso
    INSERT INTO peso_columnas (column_name, weight, updated_by)
    VALUES (p_column_name, p_weight, p_updated_by)
    ON DUPLICATE KEY UPDATE
        weight = VALUES(weight),
        updated_by = VALUES(updated_by);
END //

-- Stored Procedure para obtener pesos actuales
CREATE PROCEDURE GetColumnWeights()
BEGIN
    SELECT column_name, weight, last_updated, updated_by
    FROM peso_columnas
    ORDER BY column_name;
END //

DELIMITER ;