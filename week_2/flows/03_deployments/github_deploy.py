from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow

github_block = GitHub.load("github-store-block")

github_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-flow",
    infrastructure=github_block,
)


if __name__ == "__main__":
    github_dep.apply()