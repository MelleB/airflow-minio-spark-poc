
from faker import Faker

def evolve_fake_table(gen_row, primary_key, base_table_size, max_rows_to_delete, max_rows_to_edit, max_rows_to_add, num_generations):
    fake = Faker()
    # Set the seed so we get the same output
    Faker.seed(0)

    table = [gen_row(fake, idx) for idx in range(base_table_size)]

    for generation in range(num_generations):
        print(f"Generation {generation}")

        # Rows to delete
        num_rows_to_delete = fake.random_int(max=min(max_rows_to_delete, len(table)))
        for _ in range(num_rows_to_delete):
            idx = fake.random_int(max=len(table))
            del table[idx]
        print(f" - Deleted {num_rows_to_delete} rows")

        # Rows to edit
        num_rows_to_edit = fake.random_int(max=max_rows_to_edit)
        for _ in range(num_rows_to_edit):
            idx = fake.random_int(max=len(table))
            row = table[idx]

            keys = fake.random_elements(elements=row.keys() - primary_key, unique=True)
            new_row = gen_row(fake, idx)
            for key in keys:
                table[idx][key] = new_row[key]
        print(f" - Edited {num_rows_to_edit} rows")

        # Rows to add
        num_rows_to_add = fake.random_int(max=max_rows_to_add)
        table.extend([gen_row(fake, idx) for idx in range(num_rows_to_add)])
        print(f" - Added {num_rows_to_add} rows")

    return table

