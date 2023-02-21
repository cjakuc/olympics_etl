from olympics_etl import run_pipeline
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import RRuleSchedule
from prefect.filesystems import GitHub
from prefect.infrastructure.docker import DockerContainer

# TODO: implement github and Docker blocks
github_block = GitHub.load("github-access")
docker_container_block = DockerContainer.load("olympics-etl-container")

bucket_name='raw-csv-storage'
subfolder='raw-olympics'
local_path = 'data/raw'

rr = RRuleSchedule(rrule="FREQ=HOURLY;INTERVAL=10;COUNT=1")
deployment = Deployment.build_from_flow(
    flow=run_pipeline,
    name="RRule scheduled Python deployment via GitHub with container infrastructure",
    parameters={'bucket_name': bucket_name ,'subfolder': subfolder },
    schedule=rr,
    storage=github_block,
    infrastructure=docker_container_block
)

if __name__ == '__main__':
    deployment.apply()
