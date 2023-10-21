-- Создаем таблицу employee_data
CREATE TABLE employee_data (
    employee_key INT,
    tab_number INT,
    department VARCHAR(255),
    position VARCHAR(255),
    full_name VARCHAR(255),
    birth_date DATE,
    current_adress VARCHAR(255),
    phone1 VARCHAR(15),
    phone2 VARCHAR(15),
    work_month INT,
    work_time INT,
    start_date DATE,
    end_date DATE,
    IsCurrent BOOLEAN
);