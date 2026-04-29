CREATE OR REPLACE TEMP VIEW v_active_employees AS
SELECT * FROM employees WHERE active = true;
