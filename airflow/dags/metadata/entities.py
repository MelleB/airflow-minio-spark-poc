
from datetime import datetime
from dataclasses import dataclass
from typing import List

@dataclass
class Column:
    name: str
    source_name: str
    type: str
    pii: bool = False
    primary_key: bool = False
    optional: bool = False

@dataclass
class Trigger:
    type: str
    pattern: str
    cron: str

@dataclass
class Entity:
    name: str
    application: str
    data_domain: str
    columns: List[Column]
    trigger: Trigger

EMPLOYEE = Entity(
    name='Employee',
    application='GreatEmployees',
    data_domain='HR',
    columns=[
        Column(name='employee_id', source_name='employee_id', type='str', primary_key=True),
        Column(name='first_name', source_name='first_name', type='str', pii=True),
        Column(name='last_name', source_name='last_name', type='str', pii=True),
        Column(name='email', source_name='email', type='str', pii=True),
        Column(name='phone_number', source_name='phone_number', pii=True, type='str'),
        Column(name='job_title', source_name='job_title', type='str'),

        Column(name='department', source_name='department', type='str'),
        Column(name='salary', source_name='salary', type='float'),
        Column(name='hire_date', source_name='hire_date', type='date'),
        Column(name='address', source_name='address', type='str', pii=True),
    ],
    trigger=Trigger(
        type='filedrop',
        pattern='employee_data.csv',
        cron='@daily',
    ),
)

DEPARTMENT = Entity(
    name='Department',
    application='GreatEmployees',
    data_domain='HR',
    columns=[
        Column(name='department_id', source_name='department_id', type='str', primary_key=True),
        Column(name='name', source_name='name', type='str'),
        Column(name='phone_number', source_name='phone_number', pii=True, type='str'),
        Column(name='budget', source_name='budget', type='str'),
        Column(name='address', source_name='address', type='str', pii=True),
    ],
    trigger=Trigger(
        type='filedrop',
        pattern='department_data.csv',
        cron='@daily',
    ),
)

ENTITIES = {
    EMPLOYEE.name.lower(): EMPLOYEE,
    DEPARTMENT.name.lower(): DEPARTMENT,
}

for e in ENTITIES.values():
    num_pk = sum(1 for c in e.columns if c.primary_key)
    assert num_pk == 1, f'Config error: Single primary key expected for entity {e.name}'


def path_landingzone(entity: Entity) -> str:
    return '/'.join([
        f'app={entity.application}',
        f'entity={entity.name}',
        entity.trigger.pattern
    ])

def path_bronze(entity: Entity, ds: datetime) -> str:
    return '/'.join(['s3a://bronze',
        f'domain={entity.data_domain}',
        f'entity={entity.name}',
        f'year={ds.year}',
        f'month={str(ds.month).zfill(2)}',
        f'day={str(ds.day).zfill(2)}',
    ]) + '/'

def path_silver(entity: Entity, ds: datetime) -> str:
    return '/'.join(['s3a://silver',
        f'domain={entity.data_domain}',
        f'entity={entity.name}',
        f'year={ds.year}',
        f'month={str(ds.month).zfill(2)}',
        f'day={str(ds.day).zfill(2)}',
    ]) + '/'

def prefix_path_silver(entity: Entity) -> str:
    return '/'.join(['s3a://silver',
        f'domain={entity.data_domain}',
        f'entity={entity.name}',
    ]) + '/'


def dataset_bronze(entity: Entity) -> str:
    return '/'.join(['s3a://bronze',
        f'application={entity.application}',
        f'entity={entity.name}',
        f'year=[YYYY]',
        f'month=[MM]',
        f'day=[DD]',
    ]) + '/'

def dataset_silver(entity: Entity) -> str:
    return '/'.join(['s3a://silver',
        f'domain={entity.data_domain}',
        f'entity={entity.name}',
        f'year=[YYYY]',
        f'month=[MM]',
        f'day=[DD]',
    ]) + '/'


# - name
# - interval
# - grace_time
# - application_name
# - data_domain
# - entity_name
# - [landingzone/bronze/silver]
#   - application
#   - data_domain
#   - columns
#   - dataset
#   - path