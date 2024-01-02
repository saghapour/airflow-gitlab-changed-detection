import json
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.decorators import dag

from sensors.gitlab_sensor import AwaitGitLabRepoChangeDetectionSensor

def get_project_id_branches():
    return {
        2949: 'master',
        2622: 'master'
    }

def producer_function(changed_repos: list):
    for project_id in changed_repos:
        yield json.dumps(project_id), json.dumps({'changed_repo_id': project_id})

@dag(
    dag_id='gitlab_repo_monitoring_deferrable',
    start_date=datetime(2023, 12, 1),
    catchup=False,
    schedule_interval="@once",
    render_template_as_native_obj=True
)
def gitlab_repo_monitoring_deferrable():
    gitlab_sensor = AwaitGitLabRepoChangeDetectionSensor(
        gitlab_conn_id='gitlab_conn_id',
        projects=get_project_id_branches(),
        check_runs=10,
        xcom_push_key='changed_repos',
        task_id='gitlab_repo_sensor'
    )

    producer = ProduceToTopicOperator(
        task_id='produce_to_kafka',
        kafka_config_id='kafka_conn_id',
        topic='gitlab_repo_monitoring',
        producer_function=producer_function,
        producer_function_args=["{{ ti.xcom_pull(key='changed_repos') }}"],
        trigger_rule='all_done'
    )

    dag_run = TriggerDagRunOperator(
        task_id='trigger_dag_run',
        trigger_dag_id='gitlab_repo_monitoring_deferrable',
        trigger_rule='all_done'
    )

    gitlab_sensor >> producer >> dag_run

gitlab_repo_monitoring_deferrable()