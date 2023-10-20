Notes for running, I believe dagster was really struggling to build the project correctly because of running in a previously created conda env.

To get things to work I:

- Deactivated conda
- Create a new venv with python3 -m venv mynewvenv
- Added dependencies to pyproject.toml under the [project] header as well as [tools.dagster]
- Ran pip install . from the root of the project
- Testing lazygit
