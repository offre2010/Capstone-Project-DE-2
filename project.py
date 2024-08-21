                    ## 1. Data Warehouse Schema Design



from sqlalchemy import create_engine, Column, Integer, String, Float, Date, MetaData, Table, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the PostgreSQL database connection
DATABASE_URL = "postgresql://username:password@localhost:5432/nyc_payroll"
engine = create_engine(DATABASE_URL)
Base = declarative_base()

# Define the payroll table schema
class Payroll(Base):
    __tablename__ = 'payroll'
    id = Column(Integer, primary_key=True)
    employee_id = Column(String)
    name = Column(String)
    department = Column(String)
    job_title = Column(String)
    salary = Column(Float)
    payment_date = Column(Date)

# Define an aggregate table for analysis (e.g., total payroll by department)
class DepartmentPayrollSummary(Base):
    __tablename__ = 'department_payroll_summary'
    id = Column(Integer, primary_key=True)
    department = Column(String)
    total_salary = Column(Float)

# Create tables in the database
Base.metadata.create_all(engine)



                            ## 2. Developing the ETL Pipeline with Airflow


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from sqlalchemy.orm import sessionmaker

# Function to extract data (for example, from a CSV file)
def extract(**kwargs):
    file_path = '/path/to/payroll_data.csv'
    payroll_data = pd.read_csv(file_path)
    return payroll_data

# Function to transform data (cleaning, formatting, etc.)
def transform(payroll_data, **kwargs):
    payroll_data['payment_date'] = pd.to_datetime(payroll_data['payment_date'])
    return payroll_data

# Function to load data into the database
def load(payroll_data, **kwargs):
    Session = sessionmaker(bind=engine)
    session = Session()

    for _, row in payroll_data.iterrows():
        payroll_record = Payroll(
            employee_id=row['employee_id'],
            name=row['name'],
            department=row['department'],
            job_title=row['job_title'],
            salary=row['salary'],
            payment_date=row['payment_date']
        )
        session.add(payroll_record)

    session.commit()
    session.close()

# Define the DAG (Directed Acyclic Graph)
dag = DAG(
    'nyc_payroll_etl',
    default_args={'owner': 'airflow', 'start_date': days_ago(1)},
    schedule_interval='@daily',
)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    op_args=[extract_task],
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    op_args=[transform_task],
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task



                            ##3. Developing Aggregate Tables


def create_aggregate_table():
    Session = sessionmaker(bind=engine)
    session = Session()

    # Aggregate total salary by department
    department_summaries = session.query(
        Payroll.department,
        func.sum(Payroll.salary).label('total_salary')
    ).group_by(Payroll.department).all()

    # Insert into department payroll summary table
    for summary in department_summaries:
        department_summary = DepartmentPayrollSummary(
            department=summary.department,
            total_salary=summary.total_salary
        )
        session.add(department_summary)

    session.commit()
    session.close()

create_aggregate_table()


                                ##4. Ensuring Data Quality and Consistency


def check_data_quality():
    Session = sessionmaker(bind=engine)
    session = Session()

    # Check for null values in important columns
    null_checks = session.query(Payroll).filter(
        (Payroll.employee_id == None) |
        (Payroll.salary == None)
    ).count()

    if null_checks > 0:
        raise ValueError("Data quality check failed: NULL values found!")

    # Check for salary anomalies (e.g., negative salaries)
    salary_checks = session.query(Payroll).filter(Payroll.salary < 0).count()

    if salary_checks > 0:
        raise ValueError("Data quality check failed: Negative salaries found!")

    session.close()

check_data_quality()


                                ## 5. Managing User Access


-- SQL script to create a user and grant read-only access
CREATE USER public_user WITH PASSWORD 'publicpassword';
GRANT CONNECT ON DATABASE nyc_payroll TO public_user;
GRANT USAGE ON SCHEMA public TO public_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO public_user;
