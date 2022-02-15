format:
	@echo "Formatting code with black and isort"
	@.venv/bin/pip --quiet install black isort
	@.venv/bin/black .
	@.venv/bin/isort .
