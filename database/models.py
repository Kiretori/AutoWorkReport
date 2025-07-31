from typing import Any, List, Optional
from datetime import date, time
from sqlalchemy import Date, ForeignKey, CheckConstraint, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates
from database.enums import ContractType
from dataclasses import dataclass


Base = declarative_base()
metadata = Base.metadata
MONTH_NAMES = [
    "janvier",
    "février",
    "mars",
    "avril",
    "mai",
    "juin",
    "juillet",
    "août",
    "septembre",
    "octobre",
    "novembre",
    "décembre",
]
DAY_NAMES = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]


class AbsenceType(Base):
    __tablename__ = "dim_type_absence"
    __table_args__ = {"schema": "hr_data"}

    id: Mapped[int] = mapped_column(primary_key=True)
    type_name: Mapped[str]
    paid: Mapped[bool]
    justification_required: Mapped[bool]
    max_days_per_year: Mapped[Optional[int]]
    description: Mapped[str]

    absences: Mapped[list["Absence"]] = relationship(back_populates="type_absence")


class Absence(Base):
    __tablename__ = "fact_absence"
    __table_args__ = {"schema": "hr_data"}

    id_employe: Mapped[int] = mapped_column(
        ForeignKey("hr_data.dim_employee.id"), primary_key=True
    )
    id_type_absence: Mapped[int] = mapped_column(
        ForeignKey("hr_data.dim_type_absence.id")
    )
    date_absence_id: Mapped[int] = mapped_column(
        ForeignKey("hr_data.dim_date.date_id"), primary_key=True
    )

    employee: Mapped["DimEmployee"] = relationship(back_populates="absences")
    type_absence: Mapped["AbsenceType"] = relationship(back_populates="absences")


class DailyAttendance(Base):
    __tablename__ = "fact_daily_attendance"
    __table_args__ = {"schema": "hr_data"}

    date_id: Mapped[int] = mapped_column(
        ForeignKey("hr_data.dim_date.date_id"), primary_key=True
    )
    id_employee: Mapped[int] = mapped_column(
        ForeignKey("hr_data.dim_employee.id"), primary_key=True
    )
    present: Mapped[bool]
    check_in_hour: Mapped[Optional[time]]
    check_out_hour: Mapped[Optional[time]]

    employee: Mapped["DimEmployee"] = relationship(back_populates="daily_attendance")
    date_table: Mapped["DateDimension"] = relationship(
        back_populates="daily_attendances"
    )


class DateDimension(Base):
    __tablename__ = "dim_date"
    __table_args__ = (
        CheckConstraint("trimestre BETWEEN 1 AND 4", name="check_valid_quarter"),
        CheckConstraint("mois BETWEEN 1 AND 12", name="check_valid_month"),
        CheckConstraint(
            "nom_mois IN ('janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre')",
            name="check_valid_month_name",
        ),
        CheckConstraint("jour BETWEEN 1 AND 31", name="check_valid_day"),
        CheckConstraint(
            "nom_jour IN ('lundi', 'mardi', 'mercredi', 'jeudi', 'vendredi', 'samedi', 'dimanche')",
            name="check_valid_day_name",
        ),
        CheckConstraint("jour_semaine BETWEEN 1 AND 7", name="check_valid_day_number"),
        {"schema": "hr_data"},
    )

    date_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    date_literale: Mapped[date] = mapped_column(Date, unique=True)
    annee: Mapped[int]
    trimestre: Mapped[int]
    mois: Mapped[int]
    nom_mois: Mapped[str] = mapped_column(String(15))
    jour: Mapped[int]
    nom_jour: Mapped[str] = mapped_column(String(15))
    jour_semaine: Mapped[int]
    est_ferie: Mapped[bool]

    daily_attendances: Mapped[List["DailyAttendance"]] = relationship(
        back_populates="date_table"
    )

    # Validations (in-code)
    @validates("trimestre")
    def validate_quarter(self, key, trimestre_value):
        if trimestre_value < 1 or trimestre_value > 4:
            raise ValueError(f"Invalid quarter value: {trimestre_value}")
        return trimestre_value

    @validates("mois")
    def validate_month(self, key, mois_value):
        if mois_value < 1 or mois_value > 12:
            raise ValueError(f"Invalid month value: {mois_value}")
        return mois_value

    @validates("nom_mois")
    def validate_month_name(self, key, nom_mois_value):
        if nom_mois_value.lower() not in MONTH_NAMES:
            raise ValueError(f"Invalid month name: {nom_mois_value}")
        return nom_mois_value.lower()

    @validates("jour")
    def validate_day(self, key, jour_value):
        if jour_value < 1 or jour_value > 31:
            raise ValueError(f"Invalid day value: {jour_value}")
        return jour_value

    @validates("nom_jour")
    def validate_day_name(self, key, nom_jour_value):
        if nom_jour_value.lower() not in DAY_NAMES:
            raise ValueError(f"Invalid day name: {nom_jour_value}")
        return nom_jour_value.lower()

    @validates("jour_semaine")
    def validate_day_number(self, key, jour_semaine_value):
        if jour_semaine_value < 1 or jour_semaine_value > 7:
            raise ValueError(f"Invalid day number value: {jour_semaine_value}")
        return jour_semaine_value

    def __repr__(self):
        return (
            f"<DateDimension("
            f"date_id={self.date_id}, "
            f"date_literale={self.date_literale}, "
            f"annee={self.annee}, "
            f"trimestre={self.trimestre}, "
            f"mois={self.mois}, "
            f"nom_mois='{self.nom_mois}', "
            f"jour={self.jour}, "
            f"nom_jour='{self.nom_jour}', "
            f"jour_semaine={self.jour_semaine}, "
            f"est_ferie={self.est_ferie}"
            f")>"
        )


# Employee dimension table
class DimEmployee(Base):
    __tablename__ = "dim_employee"
    __table_args__ = {"schema": "hr_data"}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    first_name: Mapped[str]
    last_name: Mapped[str]
    integration_date: Mapped[date]
    departure_date: Mapped[Optional[date]]
    hierarchical_manager_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("hr_data.dim_employee.id"), nullable=True
    )
    job_id: Mapped[int] = mapped_column(ForeignKey("hr_data.dim_job.id"))
    service_id: Mapped[int] = mapped_column(ForeignKey("hr_data.dim_service.id"))
    address: Mapped[str]
    cin: Mapped[str] = mapped_column(unique=True)
    email: Mapped[str]
    matricule: Mapped[int] = mapped_column(unique=True)
    phone: Mapped[str]
    contract_type: Mapped[ContractType]

    absences: Mapped[list["Absence"]] = relationship(back_populates="employee")
    daily_attendance: Mapped[list["DailyAttendance"]] = relationship(
        back_populates="employee"
    )


# Service dimension table
class DimService(Base):
    __tablename__ = "dim_service"
    __table_args__ = {"schema": "hr_data"}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    description: Mapped[Optional[str]]


# Job dimension table
class DimJob(Base):
    __tablename__ = "dim_job"
    __table_args__ = {"schema": "hr_data"}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    description: Mapped[str | None]


# ---------------------------------- REPORTING DATA MODELS ---------------------------------#
@dataclass(frozen=True)
class DailyReportData:
    date: int
    employees_under_8_30h: List[Any]
    employees_under_8h: List[Any]
    employees_absent: List[DimEmployee]
    absence_percentage: float


@dataclass(frozen=True)
class WeeklyReportData:
    start_date: date
    end_date: date
    employees_under_8_30h: List[Any]
    employees_under_8h: List[Any]
    employees_absent: List[DimEmployee]
    absence_percentage: float


@dataclass(frozen=True)
class EmailData:
    receiver_emails: List[str]
    subject: str
    html_content: str
    csv_file_path: str
