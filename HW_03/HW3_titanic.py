from airflow.models import DAG
from util.settings import default_settings
import util.functions as uf

with DAG(**default_settings()) as dag:
    uf.first_task(dag) >> uf.download_titanic_dataset() >> [uf.pivot_dataset(), uf.mean_fare_per_class()] >> uf.last_task(dag)
