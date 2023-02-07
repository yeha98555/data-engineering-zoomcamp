from prefect.filesystems import GitHub

# alternative to creating DockerContainer block in the UI
github_block = GitHub(
    repository="https://github.com/yeha98555/data-engineering-zoomcamp",  # insert your repository here
)
# github_block.get_directory("week_2_data_ingestion_prefect/homework/q4/")
github_block.save("dtcde-github", overwrite=True)