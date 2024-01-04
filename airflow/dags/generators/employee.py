import csv
from generators.base import evolve_fake_table

def generate_employee_csv_file(output_path, dag_start_date, execution_date):
    num_generations = (execution_date.date() - dag_start_date).days
    print(f"Num generations: {num_generations}")

    def gen_row(fake, _idx):
        return {
        'employee_id': fake.uuid4(),
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.email(),
        'phone_number': fake.phone_number(),
        'job_title': fake.job(),
        'department': fake.random_element(elements=('HR', 'Finance', 'Marketing', 'Sales', 'IT', 'R&D', 'Operations', 'Legal', 'Customer Service', 'Supply Chain')),
        'salary': fake.random_int(min=50000, max=100000, step=1000),
        'hire_date': fake.date_between(start_date='-5y', end_date='today'),
        'address': fake.address().replace('\n', '\\n'),
    }

    table = evolve_fake_table(
        gen_row=gen_row,
        primary_key='employee_id',
        base_table_size=1000,
        max_rows_to_delete=50,
        max_rows_to_edit=50,
        max_rows_to_add=50,
        num_generations=num_generations
    )

    with open(output_path, 'w', newline='\n') as csvfile:
        fieldnames = table[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for employee in table:
            writer.writerow(employee)

    print(f"Employee data has been written to {output_path}")