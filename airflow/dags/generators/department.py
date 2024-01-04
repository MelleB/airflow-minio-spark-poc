import csv
from generators.base import evolve_fake_table

def generate_department_csv_file(output_path, dag_start_date, execution_date):
    num_generations = (execution_date.date() - dag_start_date).days
    print(f"Num generations: {num_generations}")

    departments = ['HR', 'Finance', 'Marketing', 'Sales', 'IT', 'R&D', 'Operations', 'Legal', 'Customer Service', 'Supply Chain']

    def gen_row(fake, idx):
        return {
        'department_id': fake.uuid4(),
        'name': departments[idx],
        'phone_number': fake.phone_number(),
        'budget': fake.random_int(min=500000, max=1000000, step=100000),
        'address': fake.address().replace('\n', '\\n'),
    }

    table = evolve_fake_table(
        gen_row=gen_row,
        primary_key='department_id',
        base_table_size=len(departments),
        max_rows_to_add=0,
        max_rows_to_delete=0,
        max_rows_to_edit=4,
        num_generations=num_generations
    )

    with open(output_path, 'w', newline='\n') as csvfile:
        fieldnames = table[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for employee in table:
            writer.writerow(employee)

    print(f"Department data has been written to {output_path}")