from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/Gajbhiye046",
    #access_token=<my_access_token> # only required for private repos
)
block.get_directory("Data_engineering_zoomcamp") # specify a subfolder of repo
block.save("github-store-block")