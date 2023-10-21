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

-- Создаем триггер before_insert_employee_data
CREATE OR REPLACE FUNCTION before_insert_employee_data()
RETURNS TRIGGER AS $$
BEGIN
    DECLARE
        tab_num INT;
        max_emp_key INT;
        start_date_new DATE;
    BEGIN
        SELECT COUNT(*) INTO tab_num FROM t_employee WHERE tab_number = NEW.tab_number;

        IF tab_num = 0 THEN
            NEW.start_date = CURRENT_DATE;
            NEW.end_date = CURRENT_DATE;
            NEW.IsCurrent = 1;
        ELSE
            SELECT MAX(employee_key) + 1 INTO max_emp_key FROM t_employee;
            SELECT MAX(end_date) INTO start_date_new FROM t_employee WHERE tab_number = NEW.tab_number;

            NEW.employee_key = max_emp_key;
            NEW.start_date = start_date_new;
            NEW.end_date = CURRENT_DATE;
            NEW.IsCurrent = 1;
        END IF;

        RETURN NEW;
    END;
END;
$$ LANGUAGE plpgsql;

-- Создаем триггер для таблицы t_employee
CREATE TRIGGER before_insert_employee_data
BEFORE INSERT ON t_employee
FOR EACH ROW
EXECUTE FUNCTION before_insert_employee_data();

-- Создаем процедуру correct_IsCurrent
CREATE OR REPLACE FUNCTION correct_IsCurrent()
RETURNS VOID AS $$
BEGIN
    DECLARE
        emp_key INT;
    BEGIN
        SELECT employee_key INTO emp_key FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY tab_number ORDER BY employee_key DESC) AS row_num
            FROM t_employee
            WHERE IsCurrent <> 0
        ) AS t
        WHERE row_num = 2;

        UPDATE t_employee
        SET IsCurrent = 0
        WHERE employee_key = emp_key;
    END;
END;
$$ LANGUAGE plpgsql;
