-- Create materialized view for monthly employee absence
CREATE MATERIALIZED VIEW IF NOT EXISTS hr_data.mv_employee_absence_monthly AS
SELECT
	dd.annee,
	dd.mois,
	e.id AS employee_id,
	e.first_name,
	e.last_name,
	COUNT(*) AS monthly_absence
FROM hr_data.fact_absence abs
JOIN hr_data.dim_employee e ON e.id = abs.id_employe
JOIN hr_data.dim_date dd ON dd.date_id = abs.date_absence_id
GROUP BY 
    dd.annee, dd.mois, e.id, e.first_name, e.last_name;


-- Create materialized view for service monthly absence
CREATE MATERIALIZED VIEW IF NOT EXISTS hr_data.mv_service_absence_monthly AS
SELECT
	dd.annee,
	dd.mois,
	s.id,
	s.name,
	COUNT(*) AS monthly_absence
FROM hr_data.fact_absence abs
JOIN hr_data.dim_employee e ON e.id = abs.id_employe
JOIN hr_data.dim_date dd ON dd.date_id = abs.date_absence_id
JOIN hr_data.dim_service s ON s.id = e.service_id
GROUP BY dd.annee, dd.mois, s.id, s.name