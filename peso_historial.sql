-- Crear tabla para historial de cambios de pesos
DROP TABLE IF EXISTS peso_historial;

CREATE TABLE peso_historial (
    id INT AUTO_INCREMENT PRIMARY KEY,
    column_name VARCHAR(50) NOT NULL,
    peso_anterior DECIMAL(5,2) NOT NULL,
    peso_nuevo DECIMAL(5,2) NOT NULL,
    fecha_cambio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuario VARCHAR(100) DEFAULT 'system',
    motivo VARCHAR(255) DEFAULT NULL,
    ip_address VARCHAR(45) DEFAULT NULL,
    INDEX idx_column_name (column_name),
    INDEX idx_fecha_cambio (fecha_cambio),
    INDEX idx_usuario (usuario)
);

-- Procedimiento para insertar registro en historial
DELIMITER //
CREATE PROCEDURE InsertPesoHistorial(
    IN p_column_name VARCHAR(50),
    IN p_peso_anterior DECIMAL(5,2),
    IN p_peso_nuevo DECIMAL(5,2),
    IN p_usuario VARCHAR(100),
    IN p_motivo VARCHAR(255)
)
BEGIN
    INSERT INTO peso_historial (column_name, peso_anterior, peso_nuevo, usuario, motivo)
    VALUES (p_column_name, p_peso_anterior, p_peso_nuevo, p_usuario, IFNULL(p_motivo, 'Cambio manual'));
END //

-- Procedimiento para obtener historial completo
CREATE PROCEDURE GetPesoHistorial(
    IN p_column_name VARCHAR(50),
    IN p_fecha_inicio DATE,
    IN p_fecha_fin DATE,
    IN p_usuario VARCHAR(100),
    IN p_limit INT
)
BEGIN
    DECLARE sql_query TEXT;
    
    SET sql_query = 'SELECT id, column_name, peso_anterior, peso_nuevo, fecha_cambio, usuario, motivo 
                     FROM peso_historial WHERE 1=1';
    
    IF p_column_name IS NOT NULL AND p_column_name != '' THEN
        SET sql_query = CONCAT(sql_query, ' AND column_name = "', p_column_name, '"');
    END IF;
    
    IF p_fecha_inicio IS NOT NULL THEN
        SET sql_query = CONCAT(sql_query, ' AND DATE(fecha_cambio) >= "', p_fecha_inicio, '"');
    END IF;
    
    IF p_fecha_fin IS NOT NULL THEN
        SET sql_query = CONCAT(sql_query, ' AND DATE(fecha_cambio) <= "', p_fecha_fin, '"');
    END IF;
    
    IF p_usuario IS NOT NULL AND p_usuario != '' THEN
        SET sql_query = CONCAT(sql_query, ' AND usuario LIKE "%', p_usuario, '%"');
    END IF;
    
    SET sql_query = CONCAT(sql_query, ' ORDER BY fecha_cambio DESC');
    
    IF p_limit IS NOT NULL AND p_limit > 0 THEN
        SET sql_query = CONCAT(sql_query, ' LIMIT ', p_limit);
    END IF;
    
    SET @sql_query = sql_query;
    PREPARE stmt FROM @sql_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

-- Procedimiento para obtener estadísticas del historial
CREATE PROCEDURE GetPesoEstadisticas()
BEGIN
    SELECT 
        column_name,
        COUNT(*) as total_cambios,
        MIN(fecha_cambio) as primer_cambio,
        MAX(fecha_cambio) as ultimo_cambio,
        AVG(peso_nuevo) as peso_promedio,
        MIN(peso_nuevo) as peso_minimo,
        MAX(peso_nuevo) as peso_maximo
    FROM peso_historial 
    GROUP BY column_name 
    ORDER BY total_cambios DESC;
END //

DELIMITER ;