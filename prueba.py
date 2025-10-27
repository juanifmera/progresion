import pandas as pd
from bq_carrefour import MethodBQ

df = pd.DataFrame(data={
    'A':[1,2,3],
    'B':[4,5,6],
    'C':[7,8,9]
})

project_id = 'gcp-ar-cdg-datos-dev'
bq_methods = MethodBQ(project=project_id)

tabla_id = f'{project_id}.test.tabla_juan'

print(bq_methods.client)

bq_methods.upsert_df_to_bigquery(
    df=df,
    table_id=tabla_id,
    mode='append'
)

print('Exito')