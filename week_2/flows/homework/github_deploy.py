from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from etl_web_to_gcs_hw import etl_web_to_gcs

github_block = GitHub.load("github-store-block")

github_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="github-flow",
    storage=github_block,
    entrypoint = "week_2/flows/homework/etl_web_to_gcs_hw.py:etl_web_to_gcs"
)


if __name__ == "__main__":
    github_dep.apply()